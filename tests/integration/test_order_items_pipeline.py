"""Integration tests for the order_items ETL pipeline — FK check against injected orders DF."""

from datetime import date, datetime
from unittest.mock import patch

import pytest
from pyspark.sql import Row

from src.glue_jobs.order_items_job import run
from src.lib.config import config_from_glue_args
from src.lib.delta_utils import read_delta_table
from src.lib.logging_utils import get_logger
from src.lib.schema_definitions import ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA

_VALID_ARGS = {
    "JOB_NAME": "integration-order-items-job",
    "RAW_BUCKET": "test-raw",
    "PROCESSED_BUCKET": "test-processed",
    "QUARANTINE_BUCKET": "test-quarantine",
    "ARCHIVE_BUCKET": "test-archive",
    "ENVIRONMENT": "test",
}

_TS = datetime(2025, 4, 1, 8, 15)
_DATE = date(2025, 4, 1)

_ORDER_ROWS = [
    Row(order_num=1, order_id=101, user_id=11, order_timestamp=_TS, total_amount=34.50, date=_DATE),
    Row(order_num=2, order_id=102, user_id=12, order_timestamp=_TS, total_amount=17.99, date=_DATE),
]

_ITEM_ROWS = [
    Row(id=1001, order_id=101, user_id=11, days_since_prior_order=None,
        product_id=1, add_to_cart_order=1, reordered=0, order_timestamp=_TS, date=_DATE),
    Row(id=1002, order_id=101, user_id=11, days_since_prior_order=None,
        product_id=3, add_to_cart_order=2, reordered=0, order_timestamp=_TS, date=_DATE),
    Row(id=1003, order_id=102, user_id=12, days_since_prior_order=7,
        product_id=2, add_to_cart_order=1, reordered=1, order_timestamp=_TS, date=_DATE),
    Row(id=1004, order_id=102, user_id=12, days_since_prior_order=7,
        product_id=5, add_to_cart_order=2, reordered=0, order_timestamp=_TS, date=_DATE),
    Row(id=1005, order_id=999, user_id=99, days_since_prior_order=3,
        product_id=1, add_to_cart_order=1, reordered=0, order_timestamp=_TS, date=_DATE),
]


@pytest.fixture
def cfg():
    return config_from_glue_args(_VALID_ARGS)


@pytest.fixture
def log(cfg):
    return get_logger("order_items", cfg.job_name, "integration-items-exec-001", cfg.environment)


@pytest.fixture
def orders_df(spark):
    return spark.createDataFrame(_ORDER_ROWS, schema=ORDERS_SCHEMA)


@pytest.fixture
def items_df(spark):
    return spark.createDataFrame(_ITEM_ROWS, schema=ORDER_ITEMS_SCHEMA)


@patch("src.glue_jobs.order_items_job.emit_job_metrics")
class TestOrderItemsPipelineIntegration:
    def test_orphan_quarantined_valid_items_inserted(self, _m, spark, cfg, log, items_df, orders_df, tmp_path):
        # 4 valid items, 1 orphan (order_id=999 not in orders)
        result = run(spark, cfg, log, source_df=items_df, orders_df=orders_df,
                     delta_path=str(tmp_path / "delta"), quarantine_base_path=str(tmp_path / "q"))
        assert result["rejected"] == 1
        assert result["inserted"] == 4

    def test_delta_table_contains_valid_items_only(self, _m, spark, cfg, log, items_df, orders_df, tmp_path):
        delta = str(tmp_path / "delta_check")
        run(spark, cfg, log, source_df=items_df, orders_df=orders_df,
            delta_path=delta, quarantine_base_path=str(tmp_path / "q"))
        df = read_delta_table(spark, delta)
        assert df.count() == 4
        # Orphaned item (id=1005, order_id=999) must not be in Delta
        assert df.filter("id = 1005").count() == 0

    def test_delta_schema_matches_order_items_schema(self, _m, spark, cfg, log, items_df, orders_df, tmp_path):
        delta = str(tmp_path / "delta_schema")
        run(spark, cfg, log, source_df=items_df, orders_df=orders_df,
            delta_path=delta, quarantine_base_path=str(tmp_path / "q"))
        expected = {"id", "order_id", "user_id", "days_since_prior_order",
                    "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"}
        assert expected.issubset(set(read_delta_table(spark, delta).columns))

    def test_two_runs_are_idempotent(self, _m, spark, cfg, log, items_df, orders_df, tmp_path):
        delta = str(tmp_path / "delta_idem")
        q = str(tmp_path / "q_idem")
        run(spark, cfg, log, source_df=items_df, orders_df=orders_df, delta_path=delta, quarantine_base_path=q)
        result = run(spark, cfg, log, source_df=items_df, orders_df=orders_df, delta_path=delta, quarantine_base_path=q)
        assert result["inserted"] == 0
        assert read_delta_table(spark, delta).count() == 4

    def test_quarantine_path_created_for_orphan(self, _m, spark, cfg, log, items_df, orders_df, tmp_path):
        import os
        q_base = str(tmp_path / "q_orphan")
        run(spark, cfg, log, source_df=items_df, orders_df=orders_df,
            delta_path=str(tmp_path / "delta_orphan"), quarantine_base_path=q_base)
        assert os.path.exists(os.path.join(q_base, "table=order_items"))

    def test_null_days_since_prior_order_accepted(self, _m, spark, cfg, log, orders_df, tmp_path):
        # Items with null days_since_prior_order must not be quarantined
        items = spark.createDataFrame(
            [Row(id=2001, order_id=101, user_id=11, days_since_prior_order=None,
                 product_id=1, add_to_cart_order=1, reordered=0,
                 order_timestamp=_TS, date=_DATE)],
            schema=ORDER_ITEMS_SCHEMA,
        )
        result = run(spark, cfg, log, source_df=items, orders_df=orders_df,
                     delta_path=str(tmp_path / "d_null"), quarantine_base_path=str(tmp_path / "q_null"))
        assert result["rejected"] == 0
        assert result["inserted"] == 1
