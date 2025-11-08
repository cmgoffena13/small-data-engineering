from dagster import OpExecutionContext, op

from src.db import create_delta_table_if_not_exists, execute_audit_query
from src.sources.base import SourceConfig


@op(name="create_delta_table", config_schema={"config": SourceConfig})
def create_delta_table(context: OpExecutionContext) -> None:
    create_delta_table_if_not_exists(context.op_config["config"])


@op(name="audit_delta", config_schema={"config": SourceConfig})
def audit_delta(context: OpExecutionContext) -> None:
    execute_audit_query(context.op_config["config"])
