"""
Tests for TableProcessor.merge() in DataVaultShared.

Scenario
--------
1. Load *customer_merge_initial.csv* — creates the Delta table from scratch.
2. Load *customer_merge_incremental.csv* — merges on ``customer_id``:
   - C001  email updated  (UPDATE)
   - C003  country_code updated  (UPDATE)
   - C004  new record  (INSERT)
   - C002  unchanged — must still be present  (no-op)

After the merge the tests assert:
- total row count == 4
- updated fields are reflected
- unchanged row is untouched
- new row is present
"""
from pathlib import Path

import pytest

from pysparkvault.DataVaultShared import TableProcessor

DATA_DIR = Path(__file__).parent / "data" / "source"

INITIAL_CSV     = DATA_DIR / "customer_merge_initial.csv"
INCREMENTAL_CSV = DATA_DIR / "customer_merge_incremental.csv"

TABLE_NAME = "default.customer_merge"
KEY_COLS   = ["customer_id"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def processor(spark, tmp_path_factory):
    workspace = str(tmp_path_factory.mktemp("table_processor_workspace"))
    return TableProcessor(spark=spark, workspace=workspace, catalog="spark_catalog", is_local=True)


@pytest.fixture(scope="module")
def initial_df(spark):
    return spark.read.csv(str(INITIAL_CSV), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def incremental_df(spark):
    return spark.read.csv(str(INCREMENTAL_CSV), header=True, inferSchema=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_merge_creates_table_on_first_load(spark, processor, initial_df):
    """First call must create the Delta table and persist all 3 rows."""
    processor.merge(
        target_table_name=TABLE_NAME,
        source_df=initial_df,
        key_columns=KEY_COLS,
        force_recreate=True,
    )

    result = spark.read.format("delta").load(
        f"{processor.workspace}/{TABLE_NAME.replace('.', '/')}"
    )
    assert result.count() == 3, "Initial load must contain exactly 3 rows"


def test_merge_updates_existing_records(spark, processor, incremental_df):
    """After incremental merge: C001 and C003 must have updated email/country_code."""
    processor.merge(
        target_table_name=TABLE_NAME,
        source_df=incremental_df,
        key_columns=KEY_COLS,
    )

    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{TABLE_NAME.replace('.', '/')}")
    )

    rows = {r["customer_id"]: r for r in result.collect()}

    assert rows["C001"]["email"] == "alice@newdomain.com", \
        "C001 email must be updated to alice@newdomain.com"
    assert rows["C003"]["country_code"] == "ES" or rows["C003"]["email"] == "carlos@example.es", \
        "C003 must reflect the incremental update"


def test_merge_inserts_new_record(spark, processor):
    """C004 did not exist in the initial load — must be present after merge."""
    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{TABLE_NAME.replace('.', '/')}")
    )

    rows = {r["customer_id"]: r for r in result.collect()}

    assert "C004" in rows, "C004 must be inserted by the incremental merge"
    assert rows["C004"]["name"] == "Diana Park"
    assert rows["C004"]["country_code"] == "KR"


def test_merge_preserves_unchanged_record(spark, processor):
    """C002 is not present in the incremental file — its original data must be kept."""
    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{TABLE_NAME.replace('.', '/')}")
    )

    rows = {r["customer_id"]: r for r in result.collect()}

    assert "C002" in rows, "C002 must still exist after merge"
    assert rows["C002"]["email"] == "bob@example.com"
    assert rows["C002"]["country_code"] == "US"


def test_merge_total_row_count(spark, processor):
    """Final table must contain exactly 4 rows: C001, C002, C003, C004."""
    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{TABLE_NAME.replace('.', '/')}")
    )
    assert result.count() == 4, "After incremental merge total row count must be 4"


# ---------------------------------------------------------------------------
# read_from_dbt tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dbt_processor(spark, tmp_path_factory):
    """Separate processor instance for read_from_dbt tests."""
    workspace = str(tmp_path_factory.mktemp("dbt_workspace"))
    return TableProcessor(spark=spark, workspace=workspace, catalog="spark_catalog", is_local=True)


def test_read_from_dbt_plain_sql(spark, dbt_processor):
    """A model with no Jinja tags is executed as-is."""
    df = dbt_processor.read_from_dbt("SELECT 1 AS value")
    assert df.count() == 1
    assert df.collect()[0]["value"] == 1


def test_read_from_dbt_ref_resolves_to_qualified_name(spark, dbt_processor):
    """{{ ref('model') }} must expand to the fully qualified name from references dict."""
    spark.createDataFrame([(7,)], ["qty"]).write.mode("overwrite").saveAsTable("products")
    df = dbt_processor.read_from_dbt(
        "SELECT qty FROM {{ ref('products') }}",
        references={"products": "spark_catalog.default.products"},
    )
    assert df.count() == 1
    assert df.collect()[0]["qty"] == 7


def test_read_from_dbt_ref_uses_references_dict(spark, dbt_processor):
    """{{ ref('tbl') }} looks up the fully qualified name from references dict."""
    spark.createDataFrame([(99,)], ["score"]).write.mode("overwrite").saveAsTable("scores")
    df = dbt_processor.read_from_dbt(
        "SELECT score FROM {{ ref('scores') }}",
        references={"scores": "spark_catalog.default.scores"},
    )
    assert df.collect()[0]["score"] == 99


def test_read_from_dbt_source_resolves(spark, dbt_processor):
    """{{ source('default', 'events') }} must expand to spark_catalog.default.events."""
    spark.createDataFrame([("click",)], ["event_type"]).write.mode("overwrite").saveAsTable("events")
    df = dbt_processor.read_from_dbt(
        "SELECT event_type FROM {{ source('default', 'events') }}"
    )
    assert df.collect()[0]["event_type"] == "click"


def test_read_from_dbt_config_is_stripped(spark, dbt_processor):
    """{{ config(...) }} must produce no SQL token and not break execution."""
    spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").saveAsTable("cfg_test")
    df = dbt_processor.read_from_dbt(
        "{{ config(materialized='table', tags=['daily']) }}\nSELECT id FROM {{ ref('cfg_test') }}",
        references={"cfg_test": "spark_catalog.default.cfg_test"},
    )
    assert df.count() == 1


def test_read_from_dbt_var_substitution(spark, dbt_processor):
    """{{ var('threshold') }} must be replaced with the value from variables dict."""
    df = dbt_processor.read_from_dbt(
        "SELECT {{ var('threshold', 0) }} AS threshold",
        variables={"threshold": 100},
    )
    assert df.collect()[0]["threshold"] == 100


def test_read_from_dbt_var_uses_default(spark, dbt_processor):
    """{{ var('missing', 42) }} must fall back to the default when key is absent."""
    df = dbt_processor.read_from_dbt(
        "SELECT {{ var('missing', 42) }} AS val",
        variables={},
    )
    assert df.collect()[0]["val"] == 42


def test_read_from_dbt_returns_dataframe(spark, dbt_processor):
    """Return type must be a PySpark DataFrame."""
    from pyspark.sql import DataFrame
    result = dbt_processor.read_from_dbt("SELECT 'ok' AS status")
    assert isinstance(result, DataFrame)


def test_read_from_dbt_raises_error_when_model_not_in_references(spark, dbt_processor):
    """{{ ref('unknown_model') }} must raise ValueError when model is not in references dict."""
    with pytest.raises(ValueError, match="Model 'unknown_model' not found in references"):
        dbt_processor.read_from_dbt(
            "SELECT * FROM {{ ref('unknown_model') }}",
            references={"other_model": "spark_catalog.default.other_model"},
        )


# ---------------------------------------------------------------------------
# delete_unmatched tests (separate table to avoid state leakage)
# ---------------------------------------------------------------------------

DELETE_TABLE_NAME = "default.customer_merge_delete"


def test_merge_delete_unmatched_removes_missing_rows(spark, processor, initial_df, incremental_df):
    """With delete_unmatched=True, rows absent from source must be deleted.

    Initial load: C001, C002, C003.
    Incremental:  C001 (update), C003 (update), C004 (insert).
    C002 is absent from incremental → must be deleted.
    """
    processor.merge(
        target_table_name=DELETE_TABLE_NAME,
        source_df=initial_df,
        key_columns=KEY_COLS,
        force_recreate=True,
    )

    processor.merge(
        target_table_name=DELETE_TABLE_NAME,
        source_df=incremental_df,
        key_columns=KEY_COLS,
        delete_unmatched=True,
    )

    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{DELETE_TABLE_NAME.replace('.', '/')}")
    )
    rows = {r["customer_id"]: r for r in result.collect()}

    assert "C002" not in rows, "C002 must be deleted when delete_unmatched=True"
    assert result.count() == 3, "Only C001, C003, C004 must remain"


def test_merge_delete_unmatched_false_preserves_all_rows(spark, processor, initial_df, incremental_df):
    """With delete_unmatched=False (default), rows absent from source are kept."""
    processor.merge(
        target_table_name=DELETE_TABLE_NAME,
        source_df=initial_df,
        key_columns=KEY_COLS,
        force_recreate=True,
    )

    processor.merge(
        target_table_name=DELETE_TABLE_NAME,
        source_df=incremental_df,
        key_columns=KEY_COLS,
        delete_unmatched=False,
    )

    result = (
        spark.read.format("delta")
        .load(f"{processor.workspace}/{DELETE_TABLE_NAME.replace('.', '/')}")
    )
    rows = {r["customer_id"]: r for r in result.collect()}

    assert "C002" in rows, "C002 must be preserved when delete_unmatched=False"
    assert result.count() == 4
