from dagster import DynamicOut, DynamicOutput, OpExecutionContext, asset, job, op

from src.db import (
    create_delta_table_if_not_exists,
    execute_audit_query,
    load_table_from_db,
    publish_delta,
)
from src.sources.base import SourceConfig


@op(config_schema={"configs": list}, out=DynamicOut(), key_prefix=["bronze"])
def create_delta_table(context: OpExecutionContext):
    configs = context.op_config["configs"]
    for config in configs:
        create_delta_table_if_not_exists(config.schema, config)
        yield DynamicOutput(value=config, mapping_key=config.delta_table_name)


@asset(config_schema={"configs": list}, out=DynamicOut(), key_prefix=["bronze"])
def write_delta(context: OpExecutionContext):
    configs = context.op_config["configs"]
    for config in configs:
        delta_table_name = config.delta_table_name
        df = load_table_from_db(config)
        yield DynamicOutput(value=df, mapping_key=f"stage_{delta_table_name}")


@op(config_schema={"config": SourceConfig}, out=DynamicOut(), key_prefix=["bronze"])
def audit_delta(context: OpExecutionContext, config):
    execute_audit_query(config)
    yield DynamicOutput(value=config, mapping_key=config["table_name"])


@asset(config_schema={"config": SourceConfig}, key_prefix=["bronze"])
def publish_delta(context: OpExecutionContext, config):
    publish_delta(config)
    yield DynamicOutput(value=config, mapping_key=config.delta_table_name)


@job
def configurable_write_audit_publish():
    create_delta_table_results = create_delta_table()
    write_results = create_delta_table_results.map(write_delta)
    audit_results = write_results.map(audit_delta)
    audit_results.map(publish_delta)
