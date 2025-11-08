from typing import Optional, Type

from dagster import Config
from poldantic import to_polars_schema as poldantic_to_polars_schema
from pydantic import ConfigDict


class BaseSchema(Config):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    @classmethod
    def to_polars_schema(cls):
        return poldantic_to_polars_schema(cls)


class SourceConfig(Config):
    connection_string: str
    table_name: str
    table_primary_keys: list[str]
    table_schema: Type[BaseSchema]
    partition_by: Optional[list[str]] = None
    audit_query: Optional[str] = None
