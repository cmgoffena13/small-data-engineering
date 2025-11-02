from dagster import OpExecutionContext, asset, job, op

from src.db import load_table_from_db
from src.sources.base import SourceConfig


@asset(config_schema={"config": SourceConfig})
def write_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    df = load_table_from_db(config)
    df.write_delta(target="stage_" + str(config.delta_table_name), mode="overwrite")


@op(config_schema={"config": SourceConfig})
def audit_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    pass


@op(config_schema={"config": SourceConfig})
def publish_delta(context: OpExecutionContext) -> None:
    config = context.op_config["config"]
    pass


@job
def configurable_write_audit_publish() -> None:
    publish_delta(audit_delta(write_delta()))
