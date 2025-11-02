import duckdb
import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from settings import config
from src.sources.base import SourceConfig


def execute_audit_query(config: SourceConfig) -> None:
    delta_path = config.delta_table_name
    audit_query = config.audit_query.format(table=f"delta_scan(`{delta_path}`)")

    with duckdb.connect() as con:
        result = con.execute(audit_query).pl()

    failures = []

    for column in result[0]:
        if column != 1:
            failures.append(column)

    if len(failures) > 0:
        raise ValueError(f"Audit query failed for {config.table_name}: {failures}")


def create_select_statement(config: SourceConfig) -> str:
    schema = config.schema
    columns = []

    for field_name, field_info in schema.model_fields.items():
        alias = field_info.serialization_alias or field_info.alias

        if alias and alias != field_name:
            columns.append(f'{field_name} AS "{alias}"')
        else:
            columns.append(field_name)

    select_clause = ", ".join(columns)
    return f"SELECT {select_clause} FROM {config.table_name}"


def load_table_from_db(
    config: SourceConfig,
) -> pl.DataFrame:
    query = create_select_statement(config)

    return pl.read_database(
        query=query,
        connection_uri=config.connection_string,
        schema_overrides=config.schema.to_polars_schema(),
    )


def create_delta_table_if_not_exists(
    schema: str,
    source_config: SourceConfig,
    data_warehouse_path: str = config.DATA_WAREHOUSE_URL,
) -> None:
    table_uri = f"{data_warehouse_path}/{schema}/{source_config.delta_table_name}"
    try:
        DeltaTable(table_uri)
    except TableNotFoundError:
        schema = source_config.schema.to_polars_schema()
        df = pl.DataFrame(schema)
        arrow_df = df.to_arrow()
        DeltaTable.create(
            table_uri,
            schema=arrow_df.schema.to_arrow_schema(),
            partition_by=source_config.partition_by,
        )


def publish_delta(config: SourceConfig) -> None:
    source_path = f"stage_{config.delta_table_name}"
    target_path = config.delta_table_name

    predicate_parts = [
        f"target.{key} = source.{key}" for key in config.table_primary_keys
    ]
    predicate = " AND ".join(predicate_parts)

    target_table = DeltaTable(target_path)
    target_table.merge(
        source=source_path,
        predicate=predicate,
        source_alias="source",
        target_alias="target",
    ).when_matched_update_all().when_not_matched_insert_all().execute()
