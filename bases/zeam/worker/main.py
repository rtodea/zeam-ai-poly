"""
Celery Worker Application
"""
from celery import Celery
from zeam.worker_registry.core import WorkerNames # Verify registry import works
from zeam.redis_client.config import settings as redis_settings

app = Celery(
    "zeam.worker",
    broker=f"redis://{redis_settings.REDIS_HOST}:{redis_settings.REDIS_PORT}/{redis_settings.REDIS_DB}",
    backend=f"redis://{redis_settings.REDIS_HOST}:{redis_settings.REDIS_PORT}/{redis_settings.REDIS_DB}",
    include=["zeam.worker.tasks"]
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    result_backend_transport_options={
        "global_keyprefix": "zeam-recommender:"
    },
    result_extended=True
)

if __name__ == "__main__":
    app.start()
