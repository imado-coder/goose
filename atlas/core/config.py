from __future__ import annotations
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8",
                                      case_sensitive=False, extra="ignore")
    database_url: str = "postgresql://atlas:atlasdev@localhost:5432/atlas"
    db_pool_min: int = 2; db_pool_max: int = 10
    redis_url: str = "redis://localhost:6379"
    redis_stream_maxlen: int = 100_000
    binance_api_key: str = ""; binance_api_secret: str = ""
    binance_ws_base: str = "wss://fstream.binance.com"
    binance_rest_base: str = "https://fapi.binance.com"
    symbols: list[str] = Field(default=["BTCUSDT"])
    canonical_map: dict[str,str] = Field(default={"BTCUSDT":"BTC-PERP"})
    parquet_dir: str = "/data/parquet"; archive_hour_utc: int = 0
    health_port: int = 8000; heartbeat_interval_sec: float = 5.0
    gap_threshold_sec: float = 30.0; log_level: str = "INFO"

@lru_cache(maxsize=1)
def get_settings() -> Settings: return Settings()
