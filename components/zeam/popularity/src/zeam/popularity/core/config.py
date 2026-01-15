from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Popularity Recommender"
    SERVER_PORT: int = 8000
    
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None

    # Redshift
    REDSHIFT_HOST: str | None = None
    REDSHIFT_PORT: int = 5439
    REDSHIFT_DB: str | None = None
    REDSHIFT_USER: str | None = None
    REDSHIFT_PASSWORD: str | None = None
    REDSHIFT_SCHEMA: str = "public"  # Default schema

    # Worker Schedule
    WORKER_INTERVAL_MINUTES: int = 60

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()
