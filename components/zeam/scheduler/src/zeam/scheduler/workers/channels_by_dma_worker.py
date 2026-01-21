"""
Channels by DMA worker - migrated from src/popularity_recommender/worker/workers/channels_by_dma.py
Ingests channel counts by DMA from Redshift to Redis.
"""
import logging
from pathlib import Path
import json
from typing import Dict, Any

from zeam.scheduler.celery_app import app
from zeam.scheduler.workers.base_worker import BaseWorker

# Import existing infrastructure
from zeam.redshift.database import RedshiftConnection

logger = logging.getLogger(__name__)


@app.task(bind=True, name="workers.channels_by_dma_ingestion")
def channels_by_dma_ingestion(self) -> Dict[str, Any]:
    """
    Ingest channel counts by DMA from Redshift to Redis.

    Returns:
        Dict with run metadata including status and DMA count.
    """
    worker = ChannelsByDMAWorker()
    run_id = self.request.id
    task_name = "channels_by_dma_ingestion"

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")

        # Execute the worker logic
        result = worker.process()

        # Store metadata
        metadata = {
            "dma_count": result["dma_count"],
            "total_rows": result["total_rows"],
        }
        worker.store_metadata(run_id, task_name, "success", metadata)

        logger.info(f"Completed {task_name} - {result['dma_count']} DMAs processed")

        return {
            "status": "success",
            "run_id": run_id,
            **result
        }

    except Exception as e:
        logger.error(f"Failed {task_name}: {str(e)}", exc_info=True)

        # Store failure metadata
        metadata = {"error": str(e)}
        worker.store_metadata(run_id, task_name, "failed", metadata)

        raise


class ChannelsByDMAWorker(BaseWorker):
    """Worker for ingesting channel counts by DMA."""

    def __init__(self):
        super().__init__()
        self.query_path = Path(__file__).parent.parent / "sql" / "channels_by_dma.sql"

    def process(self) -> Dict[str, Any]:
        """
        Execute the channels by DMA worker logic.

        Returns:
            Dict with processing results
        """
        # 1. Execute Query
        try:
            with RedshiftConnection() as conn:
                query_text = self.query_path.read_text()
                rows = conn.execute_query(query_text)
                logger.info(f"Fetched {len(rows)} rows from Redshift")
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

        # 2. Process Data
        # Structure: Store channel counts by DMA ID
        # key: channels:dma:{dma_id}

        dma_data = {}

        for row in rows:
            # row is a dict with columns: dma_id, dma_name, channelcount
            dma_id = row['dma_id']
            dma_data[f"channels:dma:{dma_id}"] = row

        # 3. Write to Redis (using synchronous Redis client from BaseWorker)
        try:
            for key, data in dma_data.items():
                # Store as JSON string
                self.store_in_redis(key, data, ttl=60 * 60 * 25)  # 25 hours

            logger.info(f"Updated {len(dma_data)} keys in Redis")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

        return {
            "dma_count": len(dma_data),
            "total_rows": len(rows),
        }
