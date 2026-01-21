from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Popularity Recommender"
    SERVER_PORT: int = 8000
    
    # Worker Schedule
    WORKER_INTERVAL_MINUTES: int = 60

    # Worker Schedule
    WORKER_INTERVAL_MINUTES: int = 60

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()
