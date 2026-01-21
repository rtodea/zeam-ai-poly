"""
Celery Beat schedule configuration.
Define all periodic tasks here.
"""
import os
from celery.schedules import crontab, schedule
from zeam.worker_registry.core import WorkerNames

# Get worker interval from environment (default 60 minutes to match old behavior)
WORKER_INTERVAL_MINUTES = int(os.getenv("WORKER_INTERVAL_MINUTES", "60"))

# Schedule configuration
# Format: task_name: {task, schedule, options}
CELERY_BEAT_SCHEDULE = {
}


def get_beat_schedule():
    """Get the beat schedule configuration."""
    return CELERY_BEAT_SCHEDULE
