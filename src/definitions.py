from dagster import Definitions, load_assets_from_modules
from dagster_deltalake import LocalConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager

import src.orchestrations
from src.settings import config

all_assets = load_assets_from_modules([src.orchestrations])

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": DeltaLakePolarsIOManager(
            root_uri=config.DATA_WAREHOUSE_URL, storage_options=LocalConfig()
        ),
    },
)
