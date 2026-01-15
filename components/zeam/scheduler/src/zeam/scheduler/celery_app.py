"""
Celery application configuration.
"""
import os
from celery import Celery
from zeam.scheduler.beat_schedule import get_beat_schedule

# Redis connection settings
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = os.getenv("REDIS_DB", "0")

# Celery app configuration
app = Celery(
    "zeam.scheduler",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    include=["zeam.scheduler.workers"]
)

# Celery configuration
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max per task
    worker_prefetch_multiplier=1,  # One task at a time per worker
    worker_max_tasks_per_child=1000,  # Restart worker after 1000 tasks to prevent memory leaks
    beat_schedule=get_beat_schedule(),
    result_backend_transport_options={
        "global_keyprefix": "zeam-recommender:"
    },
    result_extended=True
)

# Auto-discover tasks from workers module
app.autodiscover_tasks(["zeam.scheduler.workers"])
