from dagster import Definitions, load_assets_from_modules
from dagster_deltalake_polars import DeltaLakePolarsIOManager

import src.orchestrations
from settings import config

all_assets = load_assets_from_modules(src.orchestrations)

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": DeltaLakePolarsIOManager(config.DATA_WAREHOUSE_URL),
    },
)
