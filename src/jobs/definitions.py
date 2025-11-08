from dagster import job

from src.assets.definitions import stage_table_assets, target_table_assets
from src.ops.definitions import audit_delta, create_delta_table
from src.sources.master import MASTER_REGISTRY


def make_source_sync_job_for_config(config):
    """Create a job for a specific config that sequences ops and assets."""
    # Locate assets by config keys
    stage_asset = next(
        a for a in stage_table_assets if a.key.path[-1] == f"stage_{config.table_name}"
    )
    target_asset = next(
        a for a in target_table_assets if a.key.path[-1] == config.table_name
    )

    @job(name=f"job_for_{config.table_name}")
    def job_per_config():
        created = create_delta_table(config)
        staged = stage_asset.alias("stage_asset")()
        audited = audit_delta(config)
        published = target_asset.alias("target_asset")()

        # Explicit dependencies by sequence
        staged.after(created)
        audited.after(staged)
        published.after(audited)

    return job_per_config


# Generate jobs dynamically for all configs
source_configs = MASTER_REGISTRY.get_sources()
source_sync_jobs = [
    make_source_sync_job_for_config(config) for config in source_configs
]
