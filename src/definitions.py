from dagster import Definitions, load_assets_from_modules
from dagster_deltalake_polars import DeltaLakePolarsIOManager

from settings import config
from src.assets import __all__ as asset_modules

all_assets = load_assets_from_modules(asset_modules)

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": DeltaLakePolarsIOManager(config.DATA_WAREHOUSE_URL),
    },
)
