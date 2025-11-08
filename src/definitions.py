from dagster import Definitions
from dagster_deltalake import LocalConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager

from src.assets.definitions import stage_table_assets, target_table_assets
from src.jobs.definitions import source_sync_jobs
from src.settings import config

defs = Definitions(
    assets=[*stage_table_assets, *target_table_assets],
    jobs=source_sync_jobs,
    resources={
        "io_manager": DeltaLakePolarsIOManager(
            root_uri=config.DATA_WAREHOUSE_URL, storage_options=LocalConfig()
        ),
    },
)
