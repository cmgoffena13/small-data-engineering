from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import BaseSchema, SourceConfig


class Sales(BaseSchema):
    id: int
    name: str
    quantity: int
    date: Date


SalesConfig = SourceConfig(
    table_name="sales",
    table_primary_keys=["id"],
    delta_table_name="bronze/sales.delta",
    partition_by=["date"],
    schema=Sales,
    audit_query="""
        SELECT CASE WHEN COUNT(DISTINCT id) = COUNT(*) THEN 1 ELSE 0 END AS unique_grain
        FROM {table}""",
)
