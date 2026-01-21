"""
Dummy worker - migrated from src/popularity_recommender/worker/workers/dummy.py
Ingests daily popularity data from Redshift to Redis.
"""
import logging
from pathlib import Path
from collections import defaultdict
import json
from typing import Dict, Any

from zeam.scheduler.celery_app import app
from zeam.scheduler.workers.base_worker import BaseWorker

# Import existing infrastructure
from zeam.redshift.database import RedshiftConnection
from zeam.popularity.domain.schemas import ContentItem, ContentType

logger = logging.getLogger(__name__)


@app.task(bind=True, name="workers.dummy_popularity_ingestion")
def dummy_popularity_ingestion(self) -> Dict[str, Any]:
    """
    Ingest daily popularity data from Redshift to Redis.

    Returns:
        Dict with run metadata including status and record count.
    """
    worker = DummyWorker()
    run_id = self.request.id
    task_name = "dummy_popularity_ingestion"

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")

        # Execute the worker logic
        result = worker.process()

        # Store metadata
        metadata = {
            "keys_updated": result["keys_updated"],
            "total_rows": result["total_rows"],
        }
        worker.store_metadata(run_id, task_name, "success", metadata)

        logger.info(f"Completed {task_name} - {result['total_rows']} rows processed")

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


class DummyWorker(BaseWorker):
    """Worker for ingesting daily popularity data."""

    def __init__(self):
        super().__init__()
        self.query_path = Path(__file__).parent.parent / "sql" / "dummy.sql"

    def process(self) -> Dict[str, Any]:
        """
        Execute the dummy worker logic.

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
        # Structure: Global list, and per-DMA lists
        # key: popularity:dma:{dma_id}
        # key: popularity:global

        grouped_data = defaultdict(list)

        for row in rows:
            # row is a dict
            # columns: content_type, content_id, title, dma_id

            item = ContentItem(
                id=str(row['content_id']),
                title=row['title'],
                type=ContentType(row['content_type'])
            )

            dma_id = row['dma_id']
            if dma_id:
                grouped_data[f"dummy:popularity:dma:{dma_id}"].append(item.model_dump())
            else:
                grouped_data["dummy:popularity:global"].append(item.model_dump())

        # 3. Write to Redis (using synchronous Redis client from BaseWorker)
        try:
            for key, items in grouped_data.items():
                # Store as JSON string
                self.store_in_redis(key, items, ttl=60 * 60 * 25)  # 25 hours

            logger.info(f"Updated {len(grouped_data)} keys in Redis")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

        return {
            "keys_updated": len(grouped_data),
            "total_rows": len(rows),
        }
