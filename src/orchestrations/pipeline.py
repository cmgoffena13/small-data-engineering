from dagster import OpExecutionContext, asset, job, op

from src.db import execute_audit_query, load_table_from_db, publish_delta
from src.sources.base import SourceConfig


@asset(config_schema={"config": SourceConfig})
def write_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    df = load_table_from_db(config)
    df.write_delta(target="stage_" + str(config.delta_table_name), mode="overwrite")


@op(config_schema={"config": SourceConfig})
def audit_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    execute_audit_query(config)


@op(config_schema={"config": SourceConfig})
def publish_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    publish_delta(config)


@job
def configurable_write_audit_publish() -> None:
    publish_delta(audit_delta(write_delta()))
