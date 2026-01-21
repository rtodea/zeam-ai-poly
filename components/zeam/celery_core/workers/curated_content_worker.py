"""
Worker for calculating curated content popularity.
"""
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from zeam.celery_core.core import app
from zeam.celery_core.workers.base_worker import BaseWorker
from zeam.redshift.database import RedshiftConnection

logger = logging.getLogger(__name__)


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
    worker = CuratedContentWorker()
    run_id = self.request.id
    task_name = "curated_content_popularity"
    
    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        result = worker.process(start_date, end_date, dma_id, item_count)

        return {
            "status": "success",
            "args": {
                "start_date": start_date,
                "end_date": end_date,
                "dma_id": dma_id,
                "item_count": item_count
            },
            "run_id": run_id,
            **result
        }
    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)
        raise


class CuratedContentWorker(BaseWorker):
    """Worker for processing curated content popularity."""

    def __init__(self):
        super().__init__()
        self.query_path = Path(__file__).parent.parent / "sql" / "curated_content_popularity.sql"

    def process(self, start_date: str, end_date: str, dma_id: Optional[int] = None, item_count: int = 10) -> Dict[str, Any]:
        """
        Execute the curated content popularity query.

        Args:
            start_date: Start date string (YYYY-MM-DD HH:MM:SS)
            end_date: End date string (YYYY-MM-DD HH:MM:SS)
            dma_id: Optional DMA ID to filter by
            item_count: Number of items to return (default 10)

        Returns:
            Dict with processing results
        """
        logger.info(f"Running CuratedContentWorker for period {start_date} to {end_date}, DMA: {dma_id}, Limit: {item_count}")

        try:
            query_content = self.query_path.read_text()
        except FileNotFoundError:
            logger.error(f"Query file not found at {self.query_path}")
            raise

        # Construct DMA filter and Join
        dma_filter = ""
        if dma_id:
            dma_filter = f"AND log.dmaid = {dma_id}"

        # Format the query
        formatted_query = query_content.format(
            start_date=start_date,
            end_date=end_date,
            limit=item_count,
            dma_filter=dma_filter,
        )

        rows = []
        try:
            with RedshiftConnection() as conn:
                logger.info("Executing curated content popularity query")
                rows = conn.execute_query(formatted_query)
                logger.info(f"Query returned {len(rows)} rows")
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

        # Save to Redis
        # Key format: popularity:curated:{start_date}:{end_date}:{dma_id_or_global}
        redis_key = ""
        try:
            # Extract only YYYY-MM-DD
            start_date_key = start_date.split(' ')[0]
            end_date_key = end_date.split(' ')[0]

            dma_suffix = str(dma_id) if dma_id else "global"
            redis_key = f"popularity:curated:{start_date_key}:{end_date_key}:{dma_suffix}"

            if rows:
                self.store_in_redis(redis_key, rows, ttl=60 * 60 * 24 * 8) # 8 days
            else:
                 logger.info("No rows returned, skipping Redis write.")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

        return {
            "rows_count": len(rows),
            "redis_key": redis_key,
            "params": {
                "start_date": start_date,
                "end_date": end_date,
                "dma_id": dma_id,
                "item_count": item_count
            }
        }
