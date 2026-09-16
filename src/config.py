import json
from typing import List, Union
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )

    # бд
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(default="polymarket_indexer", alias="DB_NAME")
    db_user: str = Field(default="postgres", alias="DB_USER")
    db_password: str = Field(default="postgres", alias="DB_PASSWORD")

    # ноды
    polygon_rpc_urls: Union[str, List[str]] = Field(
        default_factory=lambda: [
            "https://gateway.tenderly.co/public/polygon",
            "https://polygon-bor-rpc.publicnode.com",
            "https://polygon.drpc.org",
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
        ],
        alias="POLYGON_RPC_URLS",
    )

    target_wallet: str = Field(
        default="0x46b353667fd7d846af3bbeda6584b0e5b883d3de",
        alias="TARGET_WALLET",
    )

    start_block: int = Field(default=80_000_000, alias="START_BLOCK")
    chunk_size: int = Field(default=3000, alias="CHUNK_SIZE")
    max_retries: int = Field(default=5, alias="MAX_RETRIES")
    initial_backoff: float = Field(default=1.0, alias="INITIAL_BACKOFF")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # контракты polymarket на polygon
    ctf_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    usdc_e_address: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    native_usdc_address: str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

    # topic хеши для Transfer / TransferSingle / TransferBatch
    topic_erc20_transfer: str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    topic_erc1155_transfer_single: str = "0xc3d58168c5ae739775e10e6e5b769f22f5df370605696144f84f349dd0da6f52"
    topic_erc1155_transfer_batch: str = "0x4a39dc06dd4484083a21332cc2945d8a0c4974a32c2538169e01362e84d436a0"

    @field_validator("polygon_rpc_urls", mode="before")
    @classmethod
    def parse_rpc_urls(cls, v):
        if isinstance(v, str):
            v = v.strip()
            # может быть json массив типа ["url1","url2"]
            if v.startswith("["):
                try:
                    return json.loads(v)
                except:
                    pass
            return [u.strip() for u in v.split(",") if u.strip()]
        return v

    @model_validator(mode="after")
    def _fix_stuff(self):
        # костыль - если пароль остался дефолтный, попробовать PG_PASSWORD
        pg_pass = getattr(self, "PG_PASSWORD", None)
        if (self.db_password == "postgres" or not self.db_password) and pg_pass:
            self.db_password = pg_pass.strip("'\"")

        if isinstance(self.polygon_rpc_urls, str):
            self.polygon_rpc_urls = [u.strip() for u in self.polygon_rpc_urls.split(",") if u.strip()]
        return self

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
