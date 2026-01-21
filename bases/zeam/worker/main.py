"""
Celery Worker Application
"""
import os
from celery import Celery
from zeam.celery_core.core import TaskNames # Verify registry import works

# Redis connection settings
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = os.getenv("REDIS_DB", "0")

app = Celery(
    "zeam.worker",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
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
