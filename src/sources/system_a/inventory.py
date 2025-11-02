from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import BaseSchema, SourceConfig


class Inventory(BaseSchema):
    id: int
    name: str
    quantity: int
    date: Date


InventoryConfig = SourceConfig(
    table_name="inventory",
    table_primary_keys=["id"],
    delta_table_name="bronze/inventory.delta",
    partition_by=["date"],
    schema=Inventory,
    audit_query="""
        SELECT CASE WHEN COUNT(DISTINCT id) = COUNT(*) THEN 1 ELSE 0 END 
        FROM {table}""",
)
