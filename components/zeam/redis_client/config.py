from zeam.config.core import ZeamBaseSettings

class RedisSettings(ZeamBaseSettings):
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None

settings = RedisSettings()
