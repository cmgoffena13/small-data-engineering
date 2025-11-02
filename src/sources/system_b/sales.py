from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import BaseSchema, SourceConfig


class Sales(BaseSchema):
    id: int
    name: str
    quantity: int
    date: Date


SalesConfig = SourceConfig(
    table_name="sales",
    delta_table_path="bronze/sales.delta",
    partition_by=["date"],
    schema=Sales,
)
