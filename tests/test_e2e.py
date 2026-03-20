"""
End-to-end integration test for the RawVault pipeline.

Runs the full pipeline from raw source files:
  1. Write raw CSV extracts as Parquet to the staging area
  2. stage_table() — prepare staging (compute HKEYs, add record source)
  3. load_hub_from_prepared_staging_table() — populate hubs + satellites
  4. Assert staging and vault table contents
  5. Export all raw vault tables as CSV to tests/output/

After running `make test` (or `pytest tests/test_e2e.py`), inspect the results:

    tests/output/
    ├── hub_customer.csv
    ├── hub_order.csv
    ├── sat_customer.csv
    ├── sat_order.csv
    ├── sat_effectivity_customer.csv
    ├── sat_effectivity_order.csv
    └── lnk_customer_order.csv
"""
import shutil
from pathlib import Path

import pyspark.sql.functions as F
import pytest
from pyspark.sql.types import DoubleType, StringType

from pysparkvault.DataVaultShared import ColumnDefinition, DataVaultConventions, SatelliteDefinition
from pysparkvault.RawVault import RawVault, RawVaultConfiguration

DATA_DIR = Path(__file__).parent / "data" / "source"
OUTPUT_DIR = Path(__file__).parent / "output"
ASSERT_DIR = Path(__file__).parent / "data" / "assert"
ASSERT_STG_DIR = Path(__file__).parent / "data" / "assert" / "stg"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_raw_parquet(spark, staging_base_path: str, filename: str, csv_path: Path) -> None:
    """Write a raw source CSV as Parquet into the staging area for stage_table ingestion.

    The CSV must have columns matching the RawVaultConfiguration staging column names
    (``load_datetime``, ``load_operation``) plus the entity's business columns.
    DataVault metadata columns (HKEY, RECORD_SOURCE) must NOT be present — stage_table
    computes them.
    """
    (
        spark.read.csv(str(csv_path), header=True, inferSchema=True)
        .withColumn("load_datetime", F.to_timestamp("load_datetime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("load_operation", F.col("load_operation").cast("int"))
        .write.mode("overwrite").parquet(f"{staging_base_path}/{filename}")
    )


def _export_table(spark, db: str, table: str, label: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{label}.csv"
    (
        spark.table(f"{db}.{table}")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(out_path))
    )
    # Spark writes a directory; flatten to a single file for easy inspection
    parts = list(out_path.glob("part-*.csv"))
    if parts:
        shutil.copy(parts[0], out_path.with_suffix(".csv.tmp"))
        shutil.rmtree(out_path)
        out_path.with_suffix(".csv.tmp").rename(out_path)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def e2e_config(tmp_path_factory):
    base = str(tmp_path_factory.mktemp("e2e"))
    return RawVaultConfiguration(
        source_system_name="E2E",
        staging_base_path=f"{base}/staging",
        staging_prepared_base_path=f"{base}/staging_prepared",
        raw_base_path=f"{base}/raw",
        staging_load_date_column_name="load_datetime",
        staging_cdc_operation_column_name="load_operation",
        snapshot_override_load_date_based_on_column="snapshot_date",
        optimize_partitioning=False,
    )

@pytest.fixture(scope="module")
def conventions():
    return DataVaultConventions(
        column_prefix='dv__', 
        hub='hub__', 
        link='lnk__',
        ref='ref__',
        sat='sat__',
        pit='pit__',
        effectivity='effectivity_',
        hkey='hkey',
        hdiff='hdiff',
        load_date='ldts', 
        load_end_date='ldts_end',
        cdc_load_date = 'ldts_cdc',
        record_source='source_system',
        ref_group='group',
        deleted='is_deleted',
        valid_from='valid_from',
        valid_to='valid_to',
        cdc_operation='cdc_operation',
    )

@pytest.fixture(scope="module")
def vault(spark, e2e_config, conventions):
    rv = RawVault(spark, e2e_config, conventions)
    rv.initialize_database()
    return rv





# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------

def test_e2e_create_schema(vault):
    vault.create_hub("customer", [
        ColumnDefinition("customer_id", StringType()),
    ])
    vault.create_satellite("customer", [
        ColumnDefinition("name", StringType()),
        ColumnDefinition("email", StringType()),
        ColumnDefinition("country", StringType()),
    ])
    vault.create_hub("order", [
        ColumnDefinition("order_id", StringType()),
    ])
    vault.create_satellite("order", [
        ColumnDefinition("customer_id", StringType()),
        ColumnDefinition("amount", DoubleType()),
        ColumnDefinition("status", StringType()),
    ])
    vault.create_link("customer__order", ["customer_hkey", "order_hkey"])


# ---------------------------------------------------------------------------
# Stage raw data
# ---------------------------------------------------------------------------

def test_e2e_stage_tables(spark, vault, e2e_config, conventions):
    """Write raw Parquet files then run stage_table to produce the prepared staging layer."""
    base = e2e_config.staging_base_path
    _write_raw_parquet(spark, base, "customer.parquet", DATA_DIR / "customer_raw.csv")
    _write_raw_parquet(spark, base, "order.parquet", DATA_DIR / "order_raw.csv")

    vault.stage_table("customer", "customer.parquet", hkey_columns=["customer_id"])
    vault.stage_table("order", "order.parquet", hkey_columns=["order_id"])

    staging_db = e2e_config.staging_prepared_database_name
    customer_staging = spark.table(f"{staging_db}.customer")
    assert customer_staging.count() == 4
    assert {r[conventions.record_source_column_name()] for r in customer_staging.collect()} == {"E2E"}

    order_staging = spark.table(f"{staging_db}.order")
    assert order_staging.count() == 4
    assert {r[conventions.record_source_column_name()] for r in order_staging.collect()} == {"E2E"}

    load_date = conventions.load_date_column_name()
    _assert(spark, staging_db, "customer", ASSERT_STG_DIR / "stg_customer.csv", exclude_columns=[load_date])
    _assert(spark, staging_db, "order", ASSERT_STG_DIR / "stg_order.csv", exclude_columns=[load_date])


# ---------------------------------------------------------------------------
# Load data from staging into raw vault
# ---------------------------------------------------------------------------

def test_e2e_load_customer_hub(spark, vault, e2e_config, conventions):
    vault.load_hub_from_prepared_staging_table("customer", "customer", ["customer_id"], satellites=[
        SatelliteDefinition(
            name=conventions.sat_name("customer"),
            attributes=["name", "email", "country"],
        )
    ])

    hub_table = f"{e2e_config.raw_database_name}.{conventions.hub_name('customer')}"
    ids = {r["customer_id"] for r in spark.table(hub_table).collect()}
    assert ids == {"C001", "C002", "C003"}


def test_e2e_load_order_hub(spark, vault, e2e_config, conventions):
    vault.load_hub_from_prepared_staging_table("order", "order", ["order_id"], satellites=[
        SatelliteDefinition(
            name=conventions.sat_name("order"),
            attributes=["customer_id", "amount", "status"],
        )
    ])

    hub_table = f"{e2e_config.raw_database_name}.{conventions.hub_name('order')}"
    ids = {r["order_id"] for r in spark.table(hub_table).collect()}
    assert ids == {"O001", "O002", "O003"}


def test_e2e_hub_deduplication(spark, vault, e2e_config, conventions):
    """Reloading from the same staging table must not produce duplicate hub entries."""
    vault.load_hub_from_prepared_staging_table("customer", "customer", ["customer_id"])

    hub_table = f"{e2e_config.raw_database_name}.{conventions.hub_name('customer')}"
    assert spark.table(hub_table).count() == 3


# ---------------------------------------------------------------------------
# Export results for inspection
# ---------------------------------------------------------------------------

def test_e2e_export_output(spark, vault, e2e_config, conventions):
    """Export all raw vault tables to tests/output/ as CSV for manual inspection."""
    db = e2e_config.raw_database_name

    tables = {
        "hub_customer": conventions.hub_name("customer"),
        "hub_order": conventions.hub_name("order"),
        "sat_customer": conventions.sat_name("customer"),
        "sat_order": conventions.sat_name("order"),
        "sat_effectivity_customer": conventions.sat_effectivity_name("customer"),
        "sat_effectivity_order": conventions.sat_effectivity_name("order"),
        "lnk_customer_order": conventions.link_name("customer__order"),
    }

    for label, table in tables.items():
        _export_table(spark, db, table, label)

    stg_db = e2e_config.staging_prepared_database_name
    _export_table(spark, stg_db, "customer", "stg_customer")
    _export_table(spark, stg_db, "order", "stg_order")

    assert (OUTPUT_DIR / "hub_customer.csv").exists()
    assert (OUTPUT_DIR / "hub_order.csv").exists()
    print(f"\n✓ Output written to: {OUTPUT_DIR.resolve()}")


# ---------------------------------------------------------------------------
# Assert comparison
# ---------------------------------------------------------------------------

def _assert(spark, db: str, table: str, assert_csv: Path,
            exclude_columns: list[str]) -> None:
    """Assert that a vault table matches the corresponding assert CSV (ignoring excluded columns)."""
    from pyspark.sql.types import TimestampType

    actual = spark.table(f"{db}.{table}")
    for col in exclude_columns:
        actual = actual.drop(col)

    # Normalize timestamp columns to UTC strings so comparison is timezone-independent.
    for field in actual.schema.fields:
        if isinstance(field.dataType, TimestampType):
            actual = actual.withColumn(
                field.name,
                F.date_format(F.to_utc_timestamp(F.col(field.name), "UTC"), "yyyy-MM-dd HH:mm:ss"),
            )

    assert_columns = actual.columns
    expected = (
        spark.read.csv(str(assert_csv), header=True)
        .select(*assert_columns)
    )

    actual_rows = sorted([tuple(str(v).lower() for v in r) for r in actual.collect()])
    expected_rows = sorted([tuple(str(v).lower() for v in r) for r in expected.collect()])
    assert actual_rows == expected_rows, (
        f"Mismatch for {table}:\n  actual={actual_rows}\n  expected={expected_rows}"
    )


def test_e2e_compare_with_asserts(spark, vault, e2e_config, conventions):
    """Compare vault tables against assert CSVs, excluding runtime-generated columns."""
    db = e2e_config.raw_database_name
    load_date = conventions.load_date_column_name()
    load_end_date = conventions.load_end_date_column_name()

    _assert(
        spark, db, conventions.hub_name("customer"),
        ASSERT_DIR / "hub_customer.csv",
        exclude_columns=[load_date],
    )
    _assert(
        spark, db, conventions.hub_name("order"),
        ASSERT_DIR / "hub_order.csv",
        exclude_columns=[load_date],
    )
    _assert(
        spark, db, conventions.sat_effectivity_name("customer"),
        ASSERT_DIR / "sat_effectivity_customer.csv",
        exclude_columns=[load_date],
    )
    _assert(
        spark, db, conventions.sat_effectivity_name("order"),
        ASSERT_DIR / "sat_effectivity_order.csv",
        exclude_columns=[load_date],
    )
    _assert(
        spark, db, conventions.sat_name("customer"),
        ASSERT_DIR / "sat_customer.csv",
        exclude_columns=[load_date],
    )
    _assert(
        spark, db, conventions.sat_name("order"),
        ASSERT_DIR / "sat_order.csv",
        exclude_columns=[load_date],
    )
