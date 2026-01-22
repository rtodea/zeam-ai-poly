from zeam.config.core import ZeamBaseSettings

class RedshiftSettings(ZeamBaseSettings):
    # Redshift
    REDSHIFT_HOST: str | None = None
    REDSHIFT_PORT: int = 5439
    REDSHIFT_DB: str | None = None
    REDSHIFT_USER: str | None = None
    REDSHIFT_PASSWORD: str | None = None
    REDSHIFT_SCHEMA: str = "public"  # Default schema

settings = RedshiftSettings()
