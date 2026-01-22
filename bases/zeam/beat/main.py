"""
Celery Beat Application
"""
from celery import Celery
from zeam.redis_client.config import settings as redis_settings

app = Celery(
    "zeam.beat",
    broker=f"redis://{redis_settings.REDIS_HOST}:{redis_settings.REDIS_PORT}/{redis_settings.REDIS_DB}",
    backend=f"redis://{redis_settings.REDIS_HOST}:{redis_settings.REDIS_PORT}/{redis_settings.REDIS_DB}",
)

app.conf.update(
    timezone="UTC",
    enable_utc=True,
)
