"""
Channel stats worker - processes channel popularity statistics.
Stores per-channel metrics with dma_id, media_id, and channel_id.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from zeam.scheduler.celery_app import app
from zeam.scheduler.workers.base_worker import BaseWorker

# Import existing infrastructure
from zeam.popularity.core.database import RedshiftConnection

logger = logging.getLogger(__name__)


@app.task(bind=True, name="workers.channel_stats_ingestion")
def channel_stats_ingestion(self) -> Dict[str, Any]:
    """
    Ingest channel statistics from Redshift to Redis.
    Updates current week's channel stats daily to keep them fresh.

    Returns:
        Dict with run metadata including status and channels count.
    """
    worker = ChannelStatsWorker()
    run_id = self.request.id
    task_name = "channel_stats_ingestion"

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")

        # Execute the worker logic
        result = worker.process()

        # Store metadata
        metadata = {
            "channels_count": result["channels_count"],
            "redis_keys_created": result["redis_keys_created"],
            "week_start": result["week_start"],
            "week_end": result["week_end"],
        }
        worker.store_metadata(run_id, task_name, "success", metadata)

        logger.info(f"Completed {task_name} - {result['channels_count']} channels processed")

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


class ChannelStatsWorker(BaseWorker):
    """Worker for processing channel popularity statistics."""

    def __init__(self):
        super().__init__()
        self.query_path = Path(__file__).parent.parent / "sql" / "channel_popularity.sql"

    def get_current_week_dates(self):
        """Calculate start (Monday) and end (Sunday) of the current week."""
        today = datetime.now()
        # Monday = 0
        start_of_week = today - timedelta(days=today.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return start_of_week, end_of_week

    def process(self) -> Dict[str, Any]:
        """
        Execute the channel stats worker logic.

        Returns:
            Dict with processing results
        """
        start_date, end_date = self.get_current_week_dates()
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        formatted_start_key = start_date.strftime('%Y%m%d')
        formatted_end_key = end_date.strftime('%Y%m%d')

        logger.info(f"Running for week: {start_str} to {end_str}")

        try:
            query_content = self.query_path.read_text()
        except FileNotFoundError:
            logger.error(f"Query file not found at {self.query_path}")
            raise

        # Format the query with dates
        formatted_query = query_content.format(start_date=start_str, end_date=end_str)

        # Execute query and process results
        channel_data = {}

        try:
            with RedshiftConnection() as conn:
                logger.info("Executing channel popularity query")
                rows = conn.execute_query(formatted_query)

                logger.info(f"Query returned {len(rows)} rows")

                # Process results and aggregate by channel
                for row in rows:
                    channel_id = row["channel_id"]

                    if channel_id not in channel_data:
                        channel_data[channel_id] = {
                            "channel_id": channel_id,
                            "media_id": row["media_id"],
                            "dma_id": row["dma_id"],
                            "friendly_call_sign": row["friendly_call_sign"],
                            "dma_name": row["dma_name"],
                            "total_viewers": 0,
                            "total_sessions": 0,
                            "total_minutes": 0.0,
                            "platforms": {},
                            "site_breakdown": []
                        }

                    # Aggregate total metrics
                    channel_data[channel_id]["total_viewers"] += row["Viewers"]
                    channel_data[channel_id]["total_sessions"] += row["Sessions"]
                    channel_data[channel_id]["total_minutes"] += float(row["Minutes"])

                    # Store platform-specific data
                    platform = row["Platform"]
                    if platform not in channel_data[channel_id]["platforms"]:
                        channel_data[channel_id]["platforms"][platform] = {
                            "viewers": 0,
                            "sessions": 0,
                            "minutes": 0.0,
                        }

                    channel_data[channel_id]["platforms"][platform]["viewers"] += row["Viewers"]
                    channel_data[channel_id]["platforms"][platform]["sessions"] += row["Sessions"]
                    channel_data[channel_id]["platforms"][platform]["minutes"] += float(row["Minutes"])

                    # Store detailed breakdown by site/platform
                    channel_data[channel_id]["site_breakdown"].append({
                        "platform": platform,
                        "site_name": row["SiteName"],
                        "site_type": row["SiteType"],
                        "market_state": row["MarketState"],
                        "viewers": row["Viewers"],
                        "sessions": row["Sessions"],
                        "minutes": float(row["Minutes"]),
                        "avg": float(row["Avg"]) if row["Avg"] else 0.0
                    })

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

        if not channel_data:
            logger.warning("No channel data generated to save.")

        # Write to Redis - one key per channel
        redis_keys_created = 0
        try:
            for channel_id, data in channel_data.items():
                redis_key = f"popularity:channel:{formatted_start_key}:{formatted_end_key}:{channel_id}"
                self.store_in_redis(redis_key, data, ttl=60 * 60 * 24 * 8)  # 8 days
                redis_keys_created += 1
                logger.debug(f"Stored data for channel {channel_id} in Redis: {redis_key}")

            logger.info(f"Saved {redis_keys_created} channel stats to Redis")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

        return {
            "channels_count": len(channel_data),
            "redis_keys_created": redis_keys_created,
            "week_start": start_str,
            "week_end": end_str,
        }
