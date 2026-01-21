import logging
from typing import Dict, Any, Optional
from zeam.analytics.curated_content import get_results
from zeam.redis_client.client import store_json_data

logger = logging.getLogger(__name__)

def get_curated_content_redis_key(start_date: str, end_date: str, dma_id: Optional[int] = None) -> str:
    """
    Generates the Redis key for curated content popularity.
    """
    # Extract only YYYY-MM-DD
    # Assumes start_date and end_date are strings like "YYYY-MM-DD HH:MM:SS" or just "YYYY-MM-DD"
    start_date_key = start_date.split(' ')[0]
    end_date_key = end_date.split(' ')[0]

    dma_suffix = str(dma_id) if dma_id else "global"
    return f"zeam-recommender:popularity:curated:{start_date_key}:{end_date_key}:{dma_suffix}"

def run_curated_content_task(start_date: str, end_date: str, dma_id: Optional[int] = None, item_count: int = 10, run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes the curated content popularity logic: queries analytics and saves to Redis.
    """
    logger.info(f"Running curated content task for period {start_date} to {end_date}, DMA: {dma_id}, Limit: {item_count}. Run ID: {run_id}")

    # Execute query
    rows = get_results(start_date, end_date, dma_id, item_count)
    logger.info(f"Query returned {len(rows)} rows")

    # Save to Redis
    redis_key = get_curated_content_redis_key(start_date, end_date, dma_id)
    store_json_data(redis_key, rows)

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
