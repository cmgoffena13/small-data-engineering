import polars as pl

from src.sources.base import SourceConfig


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
