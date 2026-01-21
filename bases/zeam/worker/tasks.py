"""
Worker tasks implementation
"""
import logging
from typing import Dict, Any, Optional

from celery import shared_task
from zeam.worker_registry.core import WorkerNames
from zeam.worker_registry.curated_content import run_curated_content_task

logger = logging.getLogger(__name__)


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
    task_name = WorkerNames.CURATED_CONTENT_POPULARITY

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        return run_curated_content_task(start_date, end_date, dma_id, item_count, run_id)

    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)
        raise
