"""
Worker tasks implementation
"""
import logging
from typing import Dict, Any, Optional

from celery import shared_task
from zeam.worker_registry.core import WorkerNames
from zeam.worker_registry.curated_content import run_curated_content_task
from zeam.worker_registry.curated_content_by_platform import run_curated_content_by_platform_task


@shared_task(bind=True, name=WorkerNames.CURATED_CONTENT_POPULARITY_BY_PLATFORM)
def curated_content_popularity_by_platform(self, start_date: str, end_date: str, platformos_id: Optional[int] = None,
                                           item_count: int = 10, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Calculate curated content popularity for a given period and optionally filter by platform.
    
    Args:
        start_date: Start date string (YYYY-MM-DD HH:MM:SS)
        end_date: End date string (YYYY-MM-DD HH:MM:SS)
        platformos_id: Optional Platform OS ID to filter by
        item_count: Number of items to return (default 10)
        limit: Alias for item_count (takes precedence if provided)
    """
    run_id = self.request.id
    task_name = WorkerNames.CURATED_CONTENT_POPULARITY_BY_PLATFORM
    
    # Allow 'limit' as an alias for 'item_count'
    actual_limit = limit if limit is not None else item_count

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        return run_curated_content_by_platform_task(start_date, end_date, platformos_id, actual_limit, run_id)

    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)
        raise

logger = logging.getLogger(__name__)


@shared_task(bind=True, name=WorkerNames.CURATED_CONTENT_POPULARITY)
def curated_content_popularity(self, start_date: str, end_date: str, dma_id: Optional[int] = None, item_count: int = 10, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Calculate curated content popularity for a given period and optionally filter by DMA.
    
    Args:
        start_date: Start date string (YYYY-MM-DD HH:MM:SS)
        end_date: End date string (YYYY-MM-DD HH:MM:SS)
        dma_id: Optional DMA ID to filter by
        item_count: Number of items to return (default 10)
        limit: Alias for item_count (takes precedence if provided)
    """
    run_id = self.request.id
    task_name = WorkerNames.CURATED_CONTENT_POPULARITY
    
    # Allow 'limit' as an alias for 'item_count'
    actual_limit = limit if limit is not None else item_count

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")
        return run_curated_content_task(start_date, end_date, dma_id, actual_limit, run_id)

    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)
        raise
