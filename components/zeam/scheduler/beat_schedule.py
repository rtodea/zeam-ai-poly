"""
Celery Beat schedule configuration.
Define all periodic tasks here.
"""
import os
from celery.schedules import crontab, schedule

# Get worker interval from environment (default 60 minutes to match old behavior)
WORKER_INTERVAL_MINUTES = int(os.getenv("WORKER_INTERVAL_MINUTES", "60"))

# Schedule configuration
# Format: task_name: {task, schedule, options}
CELERY_BEAT_SCHEDULE = {
    # Dummy popularity ingestion - runs at configured interval (default every 60 minutes)
    # Migrated from src/popularity_recommender/worker/workers/dummy.py
    "dummy-popularity-ingestion": {
        "task": "workers.dummy_popularity_ingestion",
        "schedule": schedule(run_every=WORKER_INTERVAL_MINUTES * 60),  # Convert minutes to seconds
        "options": {
            "expires": WORKER_INTERVAL_MINUTES * 60 + 300,  # Expires 5 minutes after next run
        }
    },

    # Weekly stats ingestion - runs daily at midnight UTC
    # Migrated from src/popularity_recommender/worker/workers/weekly_stats.py
    "weekly-stats-ingestion": {
        "task": "workers.weekly_stats_ingestion",
        "schedule": crontab(hour=0, minute=0),  # Daily at midnight UTC
        "options": {
            "expires": 3600 * 23,  # 23 hours (before next run)
        }
    },

    # Channels by DMA ingestion - runs at configured interval (default every 60 minutes)
    # Migrated from src/popularity_recommender/worker/workers/channels_by_dma.py
    # Note: This worker was not scheduled in the old system, but is now available
    "channels-by-dma-ingestion": {
        "task": "workers.channels_by_dma_ingestion",
        "schedule": schedule(run_every=WORKER_INTERVAL_MINUTES * 60),  # Same as dummy worker
        "options": {
            "expires": WORKER_INTERVAL_MINUTES * 60 + 300,  # Expires 5 minutes after next run
        }
    },

    # Example: Run top streams processing every hour (disabled by default)
    # "process-top-streams-hourly": {
    #     "task": "workers.process_top_streams",
    #     "schedule": crontab(minute=0),  # Every hour at :00
    #     "options": {
    #         "expires": 3600,  # Task expires after 1 hour
    #     }
    # },
}


def get_beat_schedule():
    """Get the beat schedule configuration."""
    return CELERY_BEAT_SCHEDULE
