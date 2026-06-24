"""Order items ETL job — XLSX bridge, FK check against orders Delta, and Delta upsert."""

import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path

import boto3
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.data_quality.quarantine import write_to_quarantine
from src.data_quality.rules import (
    BooleanFlagRule,
    NotNullRule,
    ReferentialIntegrityRule,
    TimestampRangeRule,
    UniqueKeyRule,
)
from src.data_quality.validator import DataQualityValidator
from src.lib.config import LakehouseConfig, config_from_glue_args
from src.lib.delta_utils import (
    create_delta_table_if_not_exists,
    read_delta_table,
    write_delta_table_merge,
)
from src.lib.exceptions import LakehouseException, ValidationError
from src.lib.glue_utils import commit_job, init_glue_job, resolve_args
from src.lib.logging_utils import get_logger
from src.lib.metrics import emit_job_metrics
from src.lib.s3_utils import list_s3_objects
from src.lib.schema_definitions import ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA, PARTITION_COLS, PRIMARY_KEYS

TABLE = "order_items"
REQUIRED_ARGS = [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "QUARANTINE_BUCKET",
    "ARCHIVE_BUCKET",
    "ENVIRONMENT",
]

_TS_MIN = datetime(2020, 1, 1)
_TS_MAX = datetime(2030, 12, 31)


def _build_rules(orders_df: DataFrame) -> list:
    """Return the ordered validation rules for order_items.

    NotNull checks on order_id must precede ReferentialIntegrityRule so the FK
    column is guaranteed non-null before the anti-join executes.
    """
    return [
        NotNullRule("id"),
        NotNullRule("order_id"),
        NotNullRule("product_id"),
        UniqueKeyRule(["id"]),
        BooleanFlagRule("reordered"),
        ReferentialIntegrityRule("order_id", orders_df, "order_id"),
        TimestampRangeRule("order_timestamp", _TS_MIN, _TS_MAX),
    ]


def _xlsx_to_spark_df(spark: SparkSession, local_path: str) -> DataFrame:
    """Read one order_items XLSX file via pandas and cast each column to ORDER_ITEMS_SCHEMA.

    pandas reads Excel integer columns with empty cells as float64 (NaN); Spark cast
    converts those to null for IntegerType — correct for the nullable days_since_prior_order.
    """
    pdf = pd.read_excel(local_path, engine="openpyxl")
    raw = spark.createDataFrame(pdf)
    return raw.select(
        [
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in ORDER_ITEMS_SCHEMA.fields
            if f.name in raw.columns
        ]
    )


def _read_order_items_xlsx(
    spark: SparkSession,
    raw_bucket: str,
    s3_client=None,
) -> DataFrame:
    """Download all order_items .xlsx files from S3 incoming/ prefix and union into one DF."""
    if s3_client is None:
        s3_client = boto3.client("s3")

    frames: list[DataFrame] = []
    for key in list_s3_objects(raw_bucket, "incoming/"):
        if not key.lower().endswith(".xlsx") or "order_item" not in key.lower():
            continue
        local_path = f"/tmp/{Path(key).name}"
        s3_client.download_file(raw_bucket, key, local_path)
        frames.append(_xlsx_to_spark_df(spark, local_path))

    if not frames:
        return spark.createDataFrame([], ORDER_ITEMS_SCHEMA)

    result = frames[0]
    for frame in frames[1:]:
        result = result.union(frame)
    return result


def run(
    spark: SparkSession,
    cfg: LakehouseConfig,
    log,
    *,
    source_df: DataFrame | None = None,
    orders_df: DataFrame | None = None,
    delta_path: str | None = None,
    orders_delta_path: str | None = None,
    quarantine_base_path: str | None = None,
) -> dict:
    """Execute the order_items ETL pipeline; returns a summary dict of counts.

    Pass source_df and orders_df in tests to bypass S3/Delta reads entirely.
    """
    start_ts = time.time()

    if delta_path is None:
        delta_path = cfg.s3.order_items_delta_path
    if orders_delta_path is None:
        orders_delta_path = cfg.s3.orders_delta_path
    if quarantine_base_path is None:
        quarantine_base_path = f"s3://{cfg.s3.quarantine_bucket}"

    log.info("Order items job started", extra={"table": TABLE, "environment": cfg.environment})

    # --- Read source ---
    if source_df is None:
        source_df = _read_order_items_xlsx(spark, cfg.s3.raw_bucket)

    total_records = source_df.count()
    log.info("Source XLSX read complete", extra={"total_records": total_records, "table": TABLE})

    if total_records == 0:
        log.warning("No records in source — skipping processing", extra={"table": TABLE})
        return {"total": 0, "valid": 0, "rejected": 0, "inserted": 0, "updated": 0}

    # --- Load orders Delta for FK check ---
    if orders_df is None:
        log.info("Reading orders Delta for referential integrity check",
                 extra={"path": orders_delta_path})
        orders_df = read_delta_table(spark, orders_delta_path)

    log.info("Orders reference loaded", extra={"orders_count": orders_df.count()})

    # --- Validate (including FK check) ---
    validation_result = DataQualityValidator(_build_rules(orders_df), log).validate(source_df)

    # --- Quarantine ---
    if validation_result.records_failed > 0:
        write_to_quarantine(
            validation_result.invalid_df,
            quarantine_base_path,
            TABLE,
            cfg.execution_id,
            str(date.today()),
        )

    # --- Threshold check ---
    if validation_result.failure_ratio > cfg.validation.max_quarantine_ratio:
        raise ValidationError(
            f"Quarantine ratio {validation_result.failure_ratio:.2%} exceeds threshold "
            f"{cfg.validation.max_quarantine_ratio:.2%} — bookmark will NOT be committed",
            failure_ratio=validation_result.failure_ratio,
        )

    # --- Ensure Delta table exists ---
    create_delta_table_if_not_exists(
        spark, ORDER_ITEMS_SCHEMA, delta_path, PARTITION_COLS[TABLE]
    )

    # --- Upsert ---
    merge_result = write_delta_table_merge(
        spark, validation_result.valid_df, delta_path, PRIMARY_KEYS[TABLE]
    )

    duration_ms = int((time.time() - start_ts) * 1000)

    emit_job_metrics(
        job_name=cfg.job_name,
        environment=cfg.environment,
        records_in=total_records,
        records_valid=validation_result.records_passed,
        records_rejected=validation_result.records_failed,
        rows_inserted=merge_result.rows_inserted,
        rows_updated=merge_result.rows_updated,
        duration_ms=duration_ms,
    )

    log.info(
        "Order items job complete",
        extra={
            "event": "JOB_COMPLETE",
            "table": TABLE,
            "records_in": total_records,
            "records_valid": validation_result.records_passed,
            "records_rejected": validation_result.records_failed,
            "rows_inserted": merge_result.rows_inserted,
            "rows_updated": merge_result.rows_updated,
            "duration_ms": duration_ms,
        },
    )

    return {
        "total": total_records,
        "valid": validation_result.records_passed,
        "rejected": validation_result.records_failed,
        "inserted": merge_result.rows_inserted,
        "updated": merge_result.rows_updated,
    }


def main() -> None:
    """Glue entry point — resolves args, initialises Glue context, runs ETL, commits bookmark."""
    args = resolve_args(REQUIRED_ARGS)
    cfg = config_from_glue_args(args)
    log = get_logger(TABLE, cfg.job_name, cfg.execution_id, cfg.environment)

    _, glue_ctx, job = init_glue_job(cfg.job_name, args)
    spark = glue_ctx.spark_session if glue_ctx is not None else SparkSession.builder.getOrCreate()

    try:
        run(spark, cfg, log)
        commit_job(job)

    except ValidationError as exc:
        log.error(
            "Quarantine threshold exceeded — bookmark NOT committed",
            extra={"failure_ratio": exc.failure_ratio, "error": str(exc)},
        )
        sys.exit(1)

    except LakehouseException as exc:
        log.error(
            "Lakehouse error in order_items job",
            extra={"type": type(exc).__name__, "error": str(exc)},
        )
        sys.exit(1)

    except Exception as exc:
        log.error(
            "Unexpected failure in order_items job",
            extra={"error": str(exc), "traceback": traceback.format_exc()},
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
