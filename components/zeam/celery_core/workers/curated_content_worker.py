"""
Worker for calculating curated content popularity.
"""
import json
import logging
import os
from typing import Dict, Any, Optional

import redis
from zeam.celery_core.core import app
from zeam.analytics.curated_content import get_results

logger = logging.getLogger(__name__)

def get_redis_client() -> redis.Redis:
    """Initialize Redis connection."""
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))

    return redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=True
    )

@app.task(bind=True, name="workers.curated_content_popularity")
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
    task_name = "curated_content_popularity"
    
    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        logger.info(f"Running curated_content_popularity for period {start_date} to {end_date}, DMA: {dma_id}, Limit: {item_count}")

        # Execute query
        rows = get_results(start_date, end_date, dma_id, item_count)
        logger.info(f"Query returned {len(rows)} rows")

        # Save to Redis
        redis_key = ""
        try:
            # Extract only YYYY-MM-DD
            start_date_key = start_date.split(' ')[0]
            end_date_key = end_date.split(' ')[0]

            dma_suffix = str(dma_id) if dma_id else "global"
            redis_key = f"popularity:curated:{start_date_key}:{end_date_key}:{dma_suffix}"
            
            # Prefix as per BaseWorker behavior
            prefixed_key = f"zeam-recommender:{redis_key}"

            if rows:
                redis_client = get_redis_client()
                # 8 days TTL
                ttl = 60 * 60 * 24 * 8
                
                # Store
                data = json.dumps(rows)
                redis_client.setex(prefixed_key, ttl, data)
                logger.info(f"Stored data in Redis: {prefixed_key}")
            else:
                 logger.info("No rows returned, skipping Redis write.")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

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
