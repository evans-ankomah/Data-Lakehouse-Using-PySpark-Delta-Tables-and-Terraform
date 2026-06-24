"""Unit tests for the order_items ETL job — both source_df and orders_df injected."""

from datetime import date, datetime
from unittest.mock import patch

import pytest
from pyspark.sql import Row

from src.glue_jobs.order_items_job import _build_rules, _xlsx_to_spark_df, run
from src.lib.config import config_from_glue_args
from src.lib.exceptions import ValidationError
from src.lib.logging_utils import get_logger
from src.lib.schema_definitions import ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA

_TS = datetime(2025, 4, 1, 8, 15)
_DATE = date(2025, 4, 1)

_VALID_ARGS = {
    "JOB_NAME": "test-order-items-job",
    "RAW_BUCKET": "test-raw",
    "PROCESSED_BUCKET": "test-processed",
    "QUARANTINE_BUCKET": "test-quarantine",
    "ARCHIVE_BUCKET": "test-archive",
    "ENVIRONMENT": "test",
}

_ITEM_DEFAULTS = {
    "id": 1001,
    "order_id": 101,
    "user_id": 11,
    "days_since_prior_order": None,
    "product_id": 1,
    "add_to_cart_order": 1,
    "reordered": 0,
    "order_timestamp": _TS,
    "date": _DATE,
}

_ORDER_DEFAULTS = {
    "order_num": 1,
    "order_id": 101,
    "user_id": 11,
    "order_timestamp": _TS,
    "total_amount": 25.99,
    "date": _DATE,
}


@pytest.fixture
def cfg():
    return config_from_glue_args(_VALID_ARGS)


@pytest.fixture
def log(cfg):
    return get_logger("order_items", cfg.job_name, "exec-items-001", cfg.environment)


def _items_df(spark, rows: list[dict]):
    """Build a small order_items DataFrame merging each row dict with defaults."""
    full = [{**_ITEM_DEFAULTS, **r} for r in rows]
    return spark.createDataFrame([Row(**r) for r in full], schema=ORDER_ITEMS_SCHEMA)


def _orders_ref_df(spark, order_ids: list[int]):
    """Build a minimal orders DataFrame containing only the given order_ids."""
    rows = [{**_ORDER_DEFAULTS, "order_id": oid, "order_num": i + 1}
            for i, oid in enumerate(order_ids)]
    return spark.createDataFrame([Row(**r) for r in rows], schema=ORDERS_SCHEMA)


# ---------------------------------------------------------------------------
# Rule construction
# ---------------------------------------------------------------------------
class TestBuildRules:
    def test_returns_seven_rules(self, spark):
        orders = _orders_ref_df(spark, [101])
        assert len(_build_rules(orders)) == 7

    def test_null_order_id_rule_before_ri_rule(self, spark):
        orders = _orders_ref_df(spark, [101])
        names = [r.rule_name for r in _build_rules(orders)]
        null_idx = next(i for i, n in enumerate(names) if "not_null:order_id" in n)
        ri_idx = next(i for i, n in enumerate(names) if "referential_integrity" in n)
        assert null_idx < ri_idx

    def test_includes_boolean_flag_rule(self, spark):
        orders = _orders_ref_df(spark, [101])
        names = [r.rule_name for r in _build_rules(orders)]
        assert any("boolean_flag" in n for n in names)


# ---------------------------------------------------------------------------
# XLSX bridge
# ---------------------------------------------------------------------------
class TestXlsxToSparkDf:
    @pytest.fixture
    def sample_xlsx(self, tmp_path):
        """Write a minimal order_items XLSX using openpyxl."""
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id", "order_id", "user_id", "days_since_prior_order",
                   "product_id", "add_to_cart_order", "reordered",
                   "order_timestamp", "date"])
        ws.append([1001, 101, 11, None, 1, 1, 0, datetime(2025, 4, 1, 8, 15), date(2025, 4, 1)])
        ws.append([1002, 101, 11, 7, 2, 2, 1, datetime(2025, 4, 1, 8, 15), date(2025, 4, 1)])
        path = str(tmp_path / "order_items.xlsx")
        wb.save(path)
        return path

    def test_returns_correct_row_count(self, spark, sample_xlsx):
        assert _xlsx_to_spark_df(spark, sample_xlsx).count() == 2

    def test_id_column_present(self, spark, sample_xlsx):
        assert "id" in _xlsx_to_spark_df(spark, sample_xlsx).columns

    def test_days_since_prior_order_nullable(self, spark, sample_xlsx):
        df = _xlsx_to_spark_df(spark, sample_xlsx)
        # First row has null days_since_prior_order
        row = df.filter("id = 1001").first()
        assert row["days_since_prior_order"] is None


# ---------------------------------------------------------------------------
# run() with injected DataFrames
# ---------------------------------------------------------------------------
@patch("src.glue_jobs.order_items_job.emit_job_metrics")
class TestOrderItemsJobRun:
    def test_all_valid_rows_inserted(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001}, {"id": 1002}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d1"), quarantine_base_path=str(tmp_path / "q1"))
        assert result["total"] == 2
        assert result["inserted"] == 2
        assert result["rejected"] == 0

    def test_null_id_quarantined(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001}, {"id": None}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d2"), quarantine_base_path=str(tmp_path / "q2"))
        assert result["rejected"] == 1
        assert result["valid"] == 1

    def test_orphaned_order_id_quarantined(self, _m, spark, cfg, log, tmp_path):
        # order_id 999 does not exist in the orders reference
        source = _items_df(spark, [{"id": 1001, "order_id": 101},
                                   {"id": 1002, "order_id": 999}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d3"), quarantine_base_path=str(tmp_path / "q3"))
        assert result["rejected"] == 1
        assert result["valid"] == 1

    def test_invalid_reordered_flag_quarantined(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001, "reordered": 2}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d4"), quarantine_base_path=str(tmp_path / "q4"))
        assert result["rejected"] == 1

    def test_duplicate_id_quarantined(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001}, {"id": 1001, "product_id": 2}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d5"), quarantine_base_path=str(tmp_path / "q5"))
        assert result["rejected"] == 1
        assert result["valid"] == 1

    def test_null_days_since_prior_is_valid(self, _m, spark, cfg, log, tmp_path):
        # days_since_prior_order is the only nullable field — null must be accepted
        source = _items_df(spark, [{"id": 1001, "days_since_prior_order": None}])
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=source, orders_df=orders,
                     delta_path=str(tmp_path / "d6"), quarantine_base_path=str(tmp_path / "q6"))
        assert result["valid"] == 1
        assert result["rejected"] == 0

    def test_idempotent_rerun_no_new_inserts(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001}])
        orders = _orders_ref_df(spark, [101])
        delta = str(tmp_path / "d7")
        q = str(tmp_path / "q7")
        run(spark, cfg, log, source_df=source, orders_df=orders, delta_path=delta, quarantine_base_path=q)
        result = run(spark, cfg, log, source_df=source, orders_df=orders, delta_path=delta, quarantine_base_path=q)
        assert result["inserted"] == 0

    def test_exceeds_threshold_raises_validation_error(self, _m, spark, log, tmp_path):
        strict_cfg = config_from_glue_args({**_VALID_ARGS, "MAX_QUARANTINE_RATIO": "0.01"})
        # 3 orphaned items out of 4 total
        source = _items_df(spark, [
            {"id": 1001, "order_id": 101},
            {"id": 1002, "order_id": 999},
            {"id": 1003, "order_id": 998},
            {"id": 1004, "order_id": 997},
        ])
        orders = _orders_ref_df(spark, [101])
        with pytest.raises(ValidationError):
            run(spark, strict_cfg, log, source_df=source, orders_df=orders,
                delta_path=str(tmp_path / "d8"), quarantine_base_path=str(tmp_path / "q8"))

    def test_empty_orders_quarantines_all_items(self, _m, spark, cfg, log, tmp_path):
        source = _items_df(spark, [{"id": 1001, "order_id": 101}])
        # Empty orders reference — every item is an orphan
        empty_orders = spark.createDataFrame([], ORDERS_SCHEMA)
        result = run(spark, cfg, log, source_df=source, orders_df=empty_orders,
                     delta_path=str(tmp_path / "d9"), quarantine_base_path=str(tmp_path / "q9"))
        assert result["rejected"] == 1

    def test_empty_source_returns_zeros(self, _m, spark, cfg, log, tmp_path):
        empty = spark.createDataFrame([], ORDER_ITEMS_SCHEMA)
        orders = _orders_ref_df(spark, [101])
        result = run(spark, cfg, log, source_df=empty, orders_df=orders,
                     delta_path=str(tmp_path / "d10"), quarantine_base_path=str(tmp_path / "q10"))
        assert result == {"total": 0, "valid": 0, "rejected": 0, "inserted": 0, "updated": 0}
