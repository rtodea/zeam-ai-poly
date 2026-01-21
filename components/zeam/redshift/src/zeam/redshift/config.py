from pydantic_settings import BaseSettings, SettingsConfigDict

class RedshiftSettings(BaseSettings):
    # Redshift
    REDSHIFT_HOST: str | None = None
    REDSHIFT_PORT: int = 5439
    REDSHIFT_DB: str | None = None
    REDSHIFT_USER: str | None = None
    REDSHIFT_PASSWORD: str | None = None
    REDSHIFT_SCHEMA: str = "public"  # Default schema

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = RedshiftSettings()
