"""
Worker tasks implementation
"""
import json
import logging
import os
from typing import Dict, Any, Optional

from celery import shared_task
from zeam.worker_registry.core import WorkerNames
from zeam.analytics.curated_content import get_results
from zeam.redis_client.client import get_redis_client

logger = logging.getLogger(__name__)


def save_to_redis(redis_key, rows):
    if not rows:
        logger.info("No rows returned, skipping Redis write.")
        return

    redis_client = get_redis_client()
    redis_client.set(redis_key, json.dumps(rows))
    logger.info(f"Stored data in Redis: {redis_key}")


def to_redis_key(start_date, end_date, dma_id):
    # Extract only YYYY-MM-DD
    start_date_key = start_date.split(' ')[0]
    end_date_key = end_date.split(' ')[0]

    dma_suffix = str(dma_id) if dma_id else "global"
    redis_key = f"zeam-recommender:popularity:curated:{start_date_key}:{end_date_key}:{dma_suffix}"


@shared_task(bind=True, name=WorkerNames.CURATED_CONTENT_POPULARITY)
def curated_content_popularity(self, start_date: str, end_date: str, dma_id: Optional[int] = None, item_count: int = 10) -> Dict[str, Any]:
    """
    Calculate curated content popularity for a given period and optionally filter by DMA.
    
    Args:
        start_date: Start date string (YYYY-MM-DD HH:MM:SS)
        end_date: End date string (YYYY-MM-DD HH:MM:SS)
        dma_id: Optional DMA ID to filter by
        item_count: Number of items to return (default 10)
    """
    run_id = self.request.id
    # We can use the registry name or just string log
    task_name = WorkerNames.CURATED_CONTENT_POPULARITY

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        logger.info(f"Running for period {start_date} to {end_date}, DMA: {dma_id}, Limit: {item_count}")

        # Execute query
        rows = get_results(start_date, end_date, dma_id, item_count)
        logger.info(f"Query returned {len(rows)} rows")

        # Save to Redis
        redis_key = to_redis_key(start_date, end_date, dma_id)
        save_to_redis(redis_key, rows)

        return {
            "status": "success",
            "args": {
                "start_date": start_date,
                "end_date": end_date,
                "dma_id": dma_id,
                "item_count": item_count
            },
            "run_id": run_id,
            "rows_count": len(rows),
            "redis_key": redis_key,
        }

    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)
        raise
