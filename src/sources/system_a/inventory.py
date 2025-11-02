from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import BaseSchema, SourceConfig


class Inventory(BaseSchema):
    id: int
    name: str
    quantity: int
    date: Date


InventoryConfig = SourceConfig(
    table_name="inventory",
    delta_table_path="bronze/inventory.delta",
    partition_by=["date"],
    schema=Inventory,
)
