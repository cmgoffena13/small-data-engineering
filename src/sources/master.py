from src.sources.registry import SourceRegistry
from src.sources.system_a.inventory import InventoryConfig
from src.sources.system_b.sales import SalesConfig

MASTER_REGISTRY = SourceRegistry()

MASTER_REGISTRY.add_sources([InventoryConfig, SalesConfig])
