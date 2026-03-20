import pytest
from pysparkvault.RawVault import RawVaultConfiguration


@pytest.fixture
def default_config():
    return RawVaultConfiguration(
        source_system_name="TEST_SYSTEM",
        staging_base_path="/data/staging",
        staging_prepared_base_path="/data/staging_prepared",
        raw_base_path="/data/raw",
        staging_load_date_column_name="load_date",
        staging_cdc_operation_column_name="cdc_operation",
        snapshot_override_load_date_based_on_column="snapshot_date",
    )


def test_derived_database_names(default_config):
    assert default_config.staging_prepared_database_name == "test_system__staging_prepared"
    assert default_config.raw_database_name == "test_system__raw"


def test_paths_stored(default_config):
    assert default_config.staging_base_path == "/data/staging"
    assert default_config.staging_prepared_base_path == "/data/staging_prepared"
    assert default_config.raw_base_path == "/data/raw"


def test_column_names_stored(default_config):
    assert default_config.staging_load_date_column_name == "load_date"
    assert default_config.staging_cdc_operation_column_name == "cdc_operation"
    assert default_config.snapshot_override_load_date_based_on_column == "snapshot_date"


def test_default_partitioning_flags(default_config):
    assert default_config.optimize_partitioning is True
    assert default_config.partition_size == 5


def test_custom_partitioning_flags():
    config = RawVaultConfiguration(
        source_system_name="SYS",
        staging_base_path="/s",
        staging_prepared_base_path="/sp",
        raw_base_path="/r",
        staging_load_date_column_name="ld",
        staging_cdc_operation_column_name="op",
        snapshot_override_load_date_based_on_column="snap",
        optimize_partitioning=False,
        partition_size=10,
    )
    assert config.optimize_partitioning is False
    assert config.partition_size == 10


def test_database_names_are_lowercase():
    config = RawVaultConfiguration(
        source_system_name="MY_SOURCE",
        staging_base_path="/s",
        staging_prepared_base_path="/sp",
        raw_base_path="/r",
        staging_load_date_column_name="ld",
        staging_cdc_operation_column_name="op",
        snapshot_override_load_date_based_on_column="snap",
    )
    assert config.staging_prepared_database_name == config.staging_prepared_database_name.lower()
    assert config.raw_database_name == config.raw_database_name.lower()
