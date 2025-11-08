from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseConfig(BaseSettings):
    ENV_STATE: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class GlobalConfig(BaseConfig):
    DATA_WAREHOUSE_URL: str
    POSTGRES_URL: str


class DevConfig(GlobalConfig):
    DATA_WAREHOUSE_URL: str = "src/test_warehouse"
    POSTGRES_URL: str = "postgresql://smallde:smallde@localhost:5432/smallde"


class TestConfig(GlobalConfig):
    DATA_WAREHOUSE_URL: str = "src/test_warehouse"
    POSTGRES_URL: str = "postgresql://smallde:smallde@localhost:5432/smallde"


class ProdConfig(GlobalConfig):
    pass


@lru_cache()
def get_config(env_state: Optional[str]):
    configs = {"dev": DevConfig, "prod": ProdConfig, "test": TestConfig}
    if env_state not in configs:
        raise ValueError(
            f"Invalid ENV_STATE: {env_state}. Must be one of: {list(configs.keys())}"
        )
    return configs[env_state]()


config = get_config(BaseConfig().ENV_STATE)
