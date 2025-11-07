from typing import Optional

from poldantic import BaseModel as PoldanticBaseModel
from poldantic import ConfigDict
from pydantic import BaseModel


class BaseSchema(PoldanticBaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)


class SourceConfig(BaseModel):
    connection_string: str
    table_name: str
    table_primary_keys: list[str]
    delta_table_name: str
    schema: BaseSchema
    partition_by: Optional[list[str]] = None
    audit_query: Optional[str] = None
