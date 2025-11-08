import polars as pl
from dagster import OpExecutionContext, asset

from src.db import load_table_from_db, publish_delta
from src.sources.base import SourceConfig
from src.sources.master import MASTER_REGISTRY


def make_stage_table_asset(source_config: SourceConfig):
    """Create a stage_table asset for a specific config."""

    @asset(
        key_prefix=["bronze"],
        name=f"stage_{source_config.table_name}",
    )
    def stage_table_asset(context: OpExecutionContext) -> pl.DataFrame:
        """Load data from database and return as DataFrame for staging."""
        return load_table_from_db(source_config)

    return stage_table_asset


def make_target_table_asset(source_config: SourceConfig):
    """Create a target_table asset for a specific config."""

    @asset(
        key_prefix=["bronze"],
        name=source_config.table_name,
    )
    def target_table_asset(context: OpExecutionContext) -> None:
        """Publish staged data to target delta table."""
        publish_delta(source_config)

    return target_table_asset


def make_stage_table_assets(configs: list[SourceConfig]):
    """Asset factory that creates stage_table assets for each config."""
    return [make_stage_table_asset(config) for config in configs]


def make_target_table_assets(configs: list[SourceConfig]):
    """Asset factory that creates target_table assets for each config."""
    return [make_target_table_asset(config) for config in configs]


stage_table_assets = make_stage_table_assets(MASTER_REGISTRY.get_sources())
target_table_assets = make_target_table_assets(MASTER_REGISTRY.get_sources())
