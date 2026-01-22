from pydantic_settings import BaseSettings, SettingsConfigDict

class ZeamBaseSettings(BaseSettings):
    """
    Base settings class for all Zeam components.
    Automatically loads from .env file.
    """
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

class Settings(ZeamBaseSettings):
    PROJECT_NAME: str = "Popularity Recommender"
    SERVER_PORT: int = 8000
    
    # Worker Schedule
    WORKER_INTERVAL_MINUTES: int = 60
    
settings = Settings()
