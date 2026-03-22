import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField

import pyspark.sql.functions as F

from pysparkvault.RawVault import RawVault, RawVaultConfiguration
from pysparkvault.DataVaultShared import DataVaultConventions, ColumnDefinition

DATA_DIR = Path(__file__).parent / "data" / "source"


@pytest.fixture(scope="module")
def config(tmp_path_factory):
    base = str(tmp_path_factory.mktemp("raw_vault"))
    return RawVaultConfiguration(
        source_system_name="TEST",
        landing_zone_base_path=f"{base}/staging",
        staging_base_path=f"{base}/staging_prepared",
        staging_schema_name="staging__db",
        raw_base_path=f"{base}/raw",
        raw_schema_name="raw_db",
        staging_load_date_column_name="$__LOAD_DATE",
        staging_cdc_operation_column_name="$__OPERATION",
        snapshot_override_load_date_based_on_column="snapshot_date",
        optimize_partitioning=False,
    )


@pytest.fixture(scope="module")
def raw_vault(spark, config):
    rv = RawVault(spark, config)
    rv.initialize_database()
    return rv


# ---------------------------------------------------------------------------
# initialize_database
# ---------------------------------------------------------------------------

def test_initialize_database_creates_databases(spark, raw_vault, config):
    db_names = [row.namespace for row in spark.sql("SHOW DATABASES").collect()]
    assert config.staging_schema_name in db_names
    assert config.raw_schema_name in db_names


# ---------------------------------------------------------------------------
# create_hub
# ---------------------------------------------------------------------------

def test_create_hub_creates_table(spark, raw_vault, config):
    raw_vault.create_hub("CUSTOMER", [
        ColumnDefinition("customer_id", StringType()),
    ])
    conventions = DataVaultConventions()
    table_name = f"{config.raw_schema_name}.{conventions.hub_name('CUSTOMER')}"
    df = spark.table(table_name)
    assert conventions.hkey_column_name() in df.columns
    assert conventions.load_date_column_name() in df.columns
    assert conventions.record_source_column_name() in df.columns
    assert "customer_id" in df.columns


def test_create_hub_also_creates_effectivity_satellite(spark, raw_vault, config):
    conventions = DataVaultConventions()
    sat_name = f"{config.raw_schema_name}.{conventions.sat_effectivity_name('CUSTOMER')}"
    df = spark.table(sat_name)
    assert conventions.hkey_column_name() in df.columns
    assert conventions.deleted_column_name() in df.columns


def test_create_hub_idempotent(spark, raw_vault):
    # calling create_hub a second time should not raise
    raw_vault.create_hub("CUSTOMER", [
        ColumnDefinition("customer_id", StringType()),
    ])


# ---------------------------------------------------------------------------
# create_link
# ---------------------------------------------------------------------------

def test_create_link_creates_table(spark, raw_vault, config):
    raw_vault.create_hub("ORDER", [ColumnDefinition("order_id", StringType())])
    raw_vault.create_link("CUSTOMER__ORDER", ["CUSTOMER_HKEY", "ORDER_HKEY"])

    conventions = DataVaultConventions()
    table_name = f"{config.raw_schema_name}.{conventions.link_name('CUSTOMER__ORDER')}"
    df = spark.table(table_name)
    assert conventions.hkey_column_name() in df.columns
    assert "CUSTOMER_HKEY" in df.columns
    assert "ORDER_HKEY" in df.columns


def test_create_link_also_creates_effectivity_satellite(spark, raw_vault, config):
    conventions = DataVaultConventions()
    sat_name = f"{config.raw_schema_name}.{conventions.sat_effectivity_name('CUSTOMER__ORDER')}"
    df = spark.table(sat_name)
    assert conventions.hkey_column_name() in df.columns
    assert conventions.deleted_column_name() in df.columns


# ---------------------------------------------------------------------------
# create_satellite
# ---------------------------------------------------------------------------

def test_create_satellite_creates_table(spark, raw_vault, config):
    raw_vault.create_satellite("CUSTOMER", [
        ColumnDefinition("name", StringType()),
        ColumnDefinition("age", IntegerType()),
    ])
    conventions = DataVaultConventions()
    table_name = f"{config.raw_schema_name}.{conventions.sat_name('CUSTOMER')}"
    df = spark.table(table_name)
    assert conventions.hkey_column_name() in df.columns
    assert conventions.hdiff_column_name() in df.columns
    assert conventions.load_date_column_name() in df.columns
    assert "name" in df.columns
    assert "age" in df.columns


# ---------------------------------------------------------------------------
# create_effectivity_satellite
# ---------------------------------------------------------------------------

def test_create_effectivity_satellite_creates_table(spark, raw_vault, config):
    conventions = DataVaultConventions()
    # create_effectivity_satellite expects the full effectivity name, as called by create_hub
    full_name = conventions.sat_effectivity_name("CUSTOMER")  # SAT__EFFECTIVITY_CUSTOMER
    raw_vault.create_effectivity_satellite(full_name)
    table_name = f"{config.raw_schema_name}.{full_name}"
    df = spark.table(table_name)
    assert conventions.hkey_column_name() in df.columns
    assert conventions.hdiff_column_name() in df.columns
    assert conventions.deleted_column_name() in df.columns


# ---------------------------------------------------------------------------
# load_hub
# ---------------------------------------------------------------------------

def _read_supplier_csv(spark, conventions, path):
    schema = StructType([
        StructField(conventions.hkey_column_name(), StringType()),
        StructField("load_date", StringType()),
        StructField(conventions.record_source_column_name(), StringType()),
        StructField("supplier_id", StringType()),
    ])
    return spark.read.csv(str(path), header=True, schema=schema) \
        .withColumn(
            conventions.load_date_column_name(),
            F.to_timestamp(F.col("load_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ) \
        .drop("load_date") \
        .withColumn(conventions.cdc_operation_column_name(), F.lit(2))


def test_load_hub_inserts_new_records(spark, raw_vault, config):
    conventions = DataVaultConventions()
    raw_vault.create_hub("SUPPLIER", [ColumnDefinition("supplier_id", StringType())])

    staged_df = _read_supplier_csv(spark, conventions, DATA_DIR / "supplier_load_1.csv")
    raw_vault.load_hub(staged_df, "SUPPLIER", ["supplier_id"])

    hub_table = f"{config.raw_schema_name}.{conventions.hub_name('SUPPLIER')}"
    rows = spark.table(hub_table).collect()
    supplier_ids = {r["supplier_id"] for r in rows}
    assert "S001" in supplier_ids
    assert "S002" in supplier_ids


def test_load_hub_deduplicates_existing_records(spark, raw_vault, config):
    conventions = DataVaultConventions()

    # load only the first row (S001) — it was already inserted in the previous test
    schema = StructType([
        StructField(conventions.hkey_column_name(), StringType()),
        StructField("load_date", StringType()),
        StructField(conventions.record_source_column_name(), StringType()),
        StructField("supplier_id", StringType()),
    ])
    staged_df = spark.read.csv(str(DATA_DIR / "supplier_load_1.csv"), header=True, schema=schema) \
        .filter(F.col("supplier_id") == "S001") \
        .withColumn(
            conventions.load_date_column_name(),
            F.to_timestamp(F.col("load_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ) \
        .drop("load_date") \
        .withColumn(conventions.cdc_operation_column_name(), F.lit(2))

    raw_vault.load_hub(staged_df, "SUPPLIER", ["supplier_id"])

    hub_table = f"{config.raw_schema_name}.{conventions.hub_name('SUPPLIER')}"
    count_after = spark.table(hub_table).filter(F.col("supplier_id") == "S001").count()
    assert count_after == 1, "Duplicate records should not be inserted into the hub"
