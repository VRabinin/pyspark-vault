import pyspark.sql.functions as F

from typing import Any, Dict, List, Optional
from delta.tables import DeltaTable
from jinja2 import Environment, BaseLoader

from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DataType


class DataVaultFunctions:

    @staticmethod
    def hash(column_names: List[str]) -> Column:
        """
        Calculates a SHA-256 hash of provided columns.

        :param column_names - The columns which should be included in the hash.
        """
        columns = list(map(lambda c: F.col(c), column_names))
        return F.sha2(F.concat_ws(',', *columns), 256)

    @staticmethod
    def to_columns(column_names: List[str]) -> List[Column]:
        """"
        Convert a list of column names to DataFrame columns.

        :param column_names - The list if column names.
        """
        return list(map(lambda c: F.col(c), column_names))

    @staticmethod
    def to_timestamp(column_name: str='load_date', pattern: str="yyyy-MM-dd'T'HH:mm:ss'Z'") -> Column:
        """
        Converts a date str of a format (pattern) to a timestamp column.

        :param column_name - The column which contains the date string.
        :param pattern - The (java.time) pattern of the date string.
        """
        return F.to_timestamp(F.col(column_name), pattern)


class CDCOperations:

    def __init__(self, snapshot = 0, delete = 1, create = 2, before_update = 3, update = 4):
        self.SNAPSHOT = snapshot
        self.DELETE = delete
        self.CREATE = create
        self.BEFORE_UPDATE = before_update
        self.UPDATE = update


class DataVaultConventions:

    def __init__(
        self,  
        column_prefix='$__', 
        hub='HUB__', 
        link='LNK__',
        ref='REF__',
        sat='SAT__',
        pit='PIT__',
        effectivity='EFFECTIVITY_',
        hkey='HKEY',
        fkey='FKEY',
        hdiff='HDIFF',
        load_date='LOAD_DATE', 
        load_end_date='LOAD_END_DATE',
        cdc_load_date = 'CDC_LOAD_DATE',
        record_source='RECORD_SOURCE',
        ref_group='GROUP',
        deleted='DELETED',
        valid_from='VALID_FROM',
        valid_to='VALID_TO',
        cdc_operation='OPERATION',
        load_end_date_high_watermark: str = '9999-12-31 23:59:59',
        cdc_ops = CDCOperations()
        ) -> None:
        """
        Common conventions including prefixes, naming, etc. for the Data Vault.
        """

        self.COLUMN_PREFIX = column_prefix
        self.HUB = hub
        self.LINK = link
        self.REF = ref
        self.SAT = sat
        self.PIT = pit
        self.EFFECTIVTY = effectivity
        self.HKEY = hkey
        self.FKEY = fkey
        self.HDIFF = hdiff
        self.LOAD_DATE = load_date
        self.LOAD_END_DATE = load_end_date
        self.CDC_LOAD_DATE = cdc_load_date
        self.RECORD_SOURCE = record_source
        self.REF_GROUP = ref_group
        self.DELETED = deleted
        self.VALID_FROM = valid_from
        self.VALID_TO = valid_to
        self.CDC_OPERATION = cdc_operation
        self.LOAD_END_DATE_HIGH_WATERMARK = load_end_date_high_watermark 
        self.CDC_OPS = cdc_ops

    def cdc_operation_column_name(self) -> str:
        """
        Return the column name of the CDC operation (used in prepared staging tables).
        """
        return f'{self.COLUMN_PREFIX}{self.CDC_OPERATION}'

    def hdiff_column_name(self) -> str:
        """
        Return the column name for HDIFF column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.HDIFF}'

    def hkey_column_name(self) -> str:
        """
        Return the column name for HKEY column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.HKEY}'

    def load_date_column_name(self) -> str:
        """
        Return the column name for LOAD_DATE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.LOAD_DATE}'

    def load_end_date_column_name(self) -> str:
        """
        Return the column name for LOAD_END_DATE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.LOAD_END_DATE}'

    def record_source_column_name(self) -> str:
        """
        Return the column name for RECORD_SOURCE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.RECORD_SOURCE}'

    def ref_group_column_name(self) -> str:
        """
        Returns the column name for group column of shared reference tables.
        """
        return f'{self.COLUMN_PREFIX}{self.REF_GROUP}'

    def deleted_column_name(self) -> str:
        """
        Return the column name for DELETED column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.DELETED}'

    def valid_from_column_name(self) -> str:
        """
        Return the column name for VALID_FROM column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.VALID_FROM}'

    def valid_to_column_name(self) -> str:
        """
        Return the column name for VALID_TO column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.VALID_TO}'

    def cdc_load_date_column_name(self) -> str:
        """
        Return the column name for CDC_LOAD_DATE column including configured prefix.
        """
        return f'{self.COLUMN_PREFIX}{self.CDC_LOAD_DATE}'

    def hub_name(self, source_table_name: str) -> str:
        """
        Returns a name of a HUB table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """

        # source_table_name = source_table_name.upper() --

        if source_table_name.startswith(self.HUB):
            return source_table_name
        else:
            return f'{self.HUB}{source_table_name}'

    def sat_name(self, name: str) -> str:
        """
        Returns a name of a SAT (satellite) table, based on the base name. This method ensures, that the name is prefixed with the configured
        satellite prefix. If the prefix is already present, it will not be added.
        """

        # name = name.upper() -- No upper case for satellite tables, as they are used in joins and thus need to match the case of the base table

        if name.startswith(self.SAT):
            return name
        else:
            return f'{self.SAT}{name}'

    def link_name(self, name: str) -> str:
        """
        Returns a name of a LINK table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """
        # name = name.upper() -- No upper case for LINK tables, as they are used in joins and thus need to match the case of the base table

        if name.startswith(self.LINK):
            return name
        else:
            return f'{self.LINK}{name}'

    def sat_effectivity_name(self, name: str) -> str:
        """
        Returns a name of a effectivity SAT (satellite) table, based on the base name. This method ensures, that the name is prefixed with the configured
        satellite prefix. If the prefix is already present, it will not be added.
        """
        # name = name.upper(). -- No upper case for effectivity satellites, as they are used in joins and thus need to match the case of the base table

        if name.startswith(f'{self.SAT}{self.EFFECTIVTY}'):
            return name
        else:
            name = self.remove_prefix(name)
            return f'{self.SAT}{self.EFFECTIVTY}{name}'

    def pit_name(self, name: str) -> str:
        """
        Returns a name of a PIT (point-in-time-table) table, based on the base name. This method ensures, that the name is prefixed with the configured
        hub prefix. If the prefix is already present, it will not be added.
        """
        # name = name.upper() -- No upper case for PIT tables, as they are used in joins and thus need to match the case of the base table

        if name.startswith(self.PIT):
            return name
        else:
            return f'{self.PIT}{name}'

    def ref_name(self, name: str) -> str:
        """
        Returns a name of a REF (reference) table, base on the base name.  This method ensures, that the name is prefixed with the configured
        reference prefix. If the prefix is already present, it will not be added.
        """
        # name = name.upper() -- No upper case for REF tables, as they are used in joins and thus need to match the case of the base table

        if name.startswith(self.REF):
            return name
        else:
            return f'{self.REF}{name}'

    def remove_source_prefix(self, name: str):
        """
        Return a table name without its source prefix.
        """
        return name \
            .replace("CC_", "") \
            .replace("CCX_", "") \
            .replace("ALG_", "")

    def remove_prefix(self, name: str) -> str:
        """
        Return a table name without its prefix (e.g. 'HUB_FOO` will be transformed to 'FOO').
        """
        return name \
            .replace(self.HUB, '') \
            .replace(self.LINK, '') \
            .replace(self.REF, '') \
            .replace(self.SAT, '') \
            .replace(self.PIT, '')

    
class ColumnDefinition:

    def __init__(self, name: str, type: DataType, nullable: bool = False, comment: Optional[str] = None) -> None:
        """
        Value class to define a table column.
        """
        self.name = name
        self.type = type
        self.nullable = nullable
        self.comment = comment


class SatelliteDefinition:

    def __init__(self, name: str, attributes: List[str]) -> None:
        """
        A definition how a setllite is derived. Please do not use this method directly. Use DataVault#create_satellite_definition.

        :param name - The name of the satellite table in the raw vault.
        :param attributes - The name of the columns/ attributes as in source table and satellite table.
        """
        self.name = name
        self.attributes = attributes


class ColumnReference:

    def __init__(self, table: str, column: str) -> None:
        """
        Simple value class to describe a reference to a column.

        :param table - The name of the table the column belongs to.
        :üaram column - The name of the column.
        """
        self.table = table
        self.column = column


class ForeignKey:

    def __init__(self, column: str, to: ColumnReference) -> None:
        """
        Simple value class to describe a foreign key constraint.

        :param column - The name of the column which points to a foreign table/ column.
        :param to - The reference to the foreign column.
        """
        self.column = column
        self.to = to


class LinkedHubDefinition:

    def __init__(self, name: str, hkey_column_name: str, foreign_key: ForeignKey) -> None:
        """
        A value class to specify a linked hub of a Link table.

        :param name - The base name of the hub.
        :param hkey_column_name - The name of the column in the link table.
        :param foreign_key - The foreign key from the Links staging table to the staging table of the linked hub.
        """
        self.name = name
        self.hkey_column_name = hkey_column_name
        self.foreign_key = foreign_key


class TableProcessor:

    def __init__(self, spark, workspace: str, catalog: str, is_local: bool = False) -> None:
        """
        Processor for Delta table operations scoped to a workspace and catalog.

        :param spark - The active SparkSession.
        :param workspace - Base file system path for Delta table storage.
        :param catalog - Catalog name used to qualify table references.
        :param is_local - Flag indicating if the processor is running in a local environment.

        """
        self.spark = spark
        self.workspace = workspace
        self.catalog = catalog
        self.is_local = is_local
        if self.is_local:
            self.base_path = workspace
        else:
            self.base_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{self.catalog}.Lakehouse/Tables"

    def _qualified_table(self, schema: str, name: str) -> str:
        """
        Returns a fully qualified three-part table identifier.
        """
        return f"{self.catalog}.{schema}.{name}"

    def merge(
        self,
        target_table_name: str,
        source_df,
        key_columns: list,
        exclude_columns: list = None,
        delete_unmatched: bool = False,        
        force_recreate: bool = False
    ):
        """
        Merge source_df into the target Delta table using the specified key columns.
        Creates the table if it does not yet exist.

        :param target_table_name - Dot-separated table identifier, e.g. 'schema.table'.
        :param source_df          - Source DataFrame to merge from.
        :param key_columns        - Columns used to match rows between source and target.
        :param exclude_columns    - Columns to exclude from the merge payload entirely.
        :param delete_unmatched   - When True, delete target rows that have no matching row in source_df.        
        :param force_recreate     - Drop and recreate the target table before merging.
        """
        exclude_columns = exclude_columns or []

        parts = target_table_name.split(".")
        schema = parts[-2] if len(parts) > 1 else "default"
        name = parts[-1]

        target_table_path = f"{self.base_path}/{target_table_name.replace('.', '/')}"

        if force_recreate:
            self.spark.sql(f"DROP TABLE IF EXISTS {self._qualified_table(schema, name)}")

        table_exists = DeltaTable.isDeltaTable(self.spark, target_table_path)

        if not table_exists:
            source_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(target_table_path)
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self._qualified_table(schema, name)} "
                f"USING DELTA LOCATION '{target_table_path}'"
            )
            return

        target = DeltaTable.forPath(self.spark, target_table_path)

        target_alias = "t"
        source_alias = "s"

        merge_condition = " AND ".join(
            [f"{target_alias}.{col} = {source_alias}.{col}" for col in key_columns]
        )

        columns = [
            c for c in source_df.columns
            if c not in exclude_columns
        ]

        update_columns = [
            c for c in columns
            if c not in key_columns
        ]

        update_set = {col: f"{source_alias}.{col}" for col in update_columns}
        insert_values = {col: f"{source_alias}.{col}" for col in columns}

        merge_builder = (
            target.alias(target_alias)
            .merge(
                source_df.alias(source_alias),
                merge_condition
            )
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
        )

        if delete_unmatched:
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()

        merge_builder.execute()

    def read_from_dbt(
        self,
        model_sql: str,
        ref_schema: str = "default",
        variables: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        """
        Render a dbt-style Jinja SQL model and execute it via spark.sql.

        Supported Jinja helpers:
          - ``ref('model')``           — resolves to ``catalog.ref_schema.model``
          - ``source('src', 'table')`` — resolves to ``catalog.src.table``
          - ``config(...)``            — no-op (stripped from the rendered SQL)
          - ``var('name', default)``   — looks up a value from *variables*

        :param model_sql   - SQL string with Jinja tags as written in a dbt model file.
        :param ref_schema  - Default schema used when resolving ``ref()`` calls.
        :param variables   - Optional mapping of variable names to values for ``var()``.
        :returns A DataFrame produced by executing the rendered SQL.
        """
        variables = variables or {}
        catalog = self.catalog

        def _ref(model: str, schema: str = ref_schema) -> str:
            return f"{catalog}.{schema}.{model}"

        def _source(source_name: str, table_name: str) -> str:
            return f"{catalog}.{source_name}.{table_name}"

        def _config(**kwargs) -> str:  # noqa: ARG001
            return ""

        def _var(name: str, default: Any = None) -> Any:
            return variables.get(name, default)

        env = Environment(loader=BaseLoader())
        env.globals.update({
            "ref": _ref,
            "source": _source,
            "config": _config,
            "var": _var,
        })

        rendered_sql = env.from_string(model_sql).render()
        return self.spark.sql(rendered_sql)

    def get_date_spine(self, start_date: str, end_date: str) -> DataFrame:
        """
        Generate a date spine DataFrame from start_date to end_date (inclusive),
        enriched with calendar attributes (year, month, week, quarter, day-of-week).

        :param start_date - Start date string in 'yyyy-MM-dd' format.
        :param end_date   - End date string in 'yyyy-MM-dd' format.
        :returns DataFrame with one row per calendar day and derived time dimensions.
        """
        return (
            self.spark.range(1)
            .select(
                F.explode(
                    F.sequence(
                        F.to_date(F.lit(start_date)),
                        F.to_date(F.lit(end_date)),
                        F.expr("interval 1 day"),
                    )
                ).alias("td_date_id")
            )
            .select(
                "td_date_id",

                # Year / Month
                (F.year("td_date_id") * 100 + F.month("td_date_id")).alias("td_year_month_id"),
                F.date_format("td_date_id", "yyyy-MM").alias("td_year_month"),
                F.year("td_date_id").alias("td_year_id"),
                F.month("td_date_id").alias("td_month_id"),
                F.date_format("td_date_id", "MMMM").alias("td_month"),

                # Day
                F.dayofmonth("td_date_id").alias("td_day_of_month"),

                # Week
                (F.year("td_date_id") * 100 + F.weekofyear("td_date_id")).alias("td_year_week_id"),
                F.concat(
                    F.year("td_date_id"),
                    F.lit("-W"),
                    F.lpad(F.weekofyear("td_date_id"), 2, "0"),
                ).alias("td_year_week"),
                F.weekofyear("td_date_id").alias("td_week_id"),
                F.concat(
                    F.lit("W"),
                    F.lpad(F.weekofyear("td_date_id"), 2, "0"),
                ).alias("td_week"),

                # Quarter
                (F.year("td_date_id") * 10 + F.quarter("td_date_id")).alias("td_year_quarter_id"),
                F.concat(
                    F.year("td_date_id"),
                    F.lit("-Q"),
                    F.quarter("td_date_id"),
                ).alias("td_year_quarter"),
                F.quarter("td_date_id").alias("td_quarter_id"),
                F.concat(F.lit("Q"), F.quarter("td_date_id")).alias("td_quarter"),

                # Day of Week
                F.dayofweek("td_date_id").alias("td_day_of_week_id"),
                F.date_format("td_date_id", "EEEE").alias("td_day_of_week"),
            )
        )
