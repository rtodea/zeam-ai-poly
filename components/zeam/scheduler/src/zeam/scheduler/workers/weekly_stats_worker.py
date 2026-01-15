"""
Weekly stats worker - migrated from src/popularity_recommender/worker/workers/weekly_stats.py
Updates current week's statistics daily.
"""
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from zeam.scheduler.celery_app import app
from zeam.scheduler.workers.base_worker import BaseWorker

# Import existing infrastructure
from zeam.popularity.core.database import RedshiftConnection

logger = logging.getLogger(__name__)

# Debug logging
_DEBUG_LOG_PATH = Path("/tmp/debug.log")


def _debug_log(hypothesis_id, location, message, data=None):
    """Debug logging helper."""
    import json as _json
    entry = {
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data or {},
        "timestamp": datetime.now().isoformat(),
        "sessionId": "debug-session"
    }
    with open(_DEBUG_LOG_PATH, "a") as f:
        f.write(_json.dumps(entry) + "\n")


@app.task(bind=True, name="workers.weekly_stats_ingestion")
def weekly_stats_ingestion(self) -> Dict[str, Any]:
    """
    Ingest weekly statistics from Redshift to Redis.
    Updates current week's stats daily to keep them fresh.

    Returns:
        Dict with run metadata including status and sections count.
    """
    worker = WeeklyStatsWorker()
    run_id = self.request.id
    task_name = "weekly_stats_ingestion"

    try:
        logger.info(f"Starting {task_name} - Run ID: {run_id}")

        # Execute the worker logic
        result = worker.process()

        # Store metadata
        metadata = {
            "sections_count": result["sections_count"],
            "redis_key": result["redis_key"],
            "week_start": result["week_start"],
            "week_end": result["week_end"],
        }
        worker.store_metadata(run_id, task_name, "success", metadata)

        logger.info(f"Completed {task_name} - {result['sections_count']} sections processed")

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


class WeeklyStatsWorker(BaseWorker):
    """Worker for processing weekly statistics."""

    def __init__(self):
        super().__init__()
        self.query_path = Path(__file__).parent.parent / "sql" / "weekly_stats.sql"

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
        Execute the weekly stats worker logic.

        Returns:
            Dict with processing results
        """
        start_date, end_date = self.get_current_week_dates()
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

        formatted_start_date_key = start_date.strftime('%Y-%m-%d')
        redis_key = f"popularity:weekly_stats:{formatted_start_date_key}"

        logger.info(f"Running for week: {start_str} to {end_str}")

        try:
            query_content = self.query_path.read_text()
        except FileNotFoundError:
            logger.error(f"Query file not found at {self.query_path}")
            raise

        # Format the query with dates
        formatted_query = query_content.format(start_date=start_str, end_date=end_str)

        # Split queries by semicolon to execute them sequentially
        statements = formatted_query.split(';')

        _debug_log("B", "weekly_stats_worker.py:split", "SQL split into statements", {
            "num_statements": len(statements),
            "statement_lengths": [len(s.strip()) for s in statements]
        })

        results_map = {}

        try:
            # Use context manager to keep connection open (for temp tables)
            with RedshiftConnection() as conn:
                for idx, statement in enumerate(statements):
                    statement = statement.strip()
                    if not statement:
                        continue

                    # Extract title from comments and check if statement has actual SQL
                    lines = statement.splitlines()
                    title = None
                    has_sql = False

                    for line in lines:
                        stripped = line.strip()
                        if stripped.startswith('--'):
                            if not title:
                                title = stripped.lstrip('-').strip()
                        elif stripped:  # Non-empty, non-comment line
                            has_sql = True

                    # Skip if only comments
                    if not has_sql:
                        logger.debug(f"Skipping statement {idx} - only comments")
                        continue

                    # Remove comment lines from statement before execution
                    sql_lines = []
                    for line in lines:
                        stripped = line.strip()
                        if not stripped.startswith('--'):
                            sql_lines.append(line)

                    clean_statement = '\n'.join(sql_lines).strip()
                    
                    # Double check the cleaned statement is not empty
                    if not clean_statement:
                        logger.debug(f"Skipping statement {idx} - empty after removing comments")
                        continue

                    _debug_log("D,E", f"weekly_stats_worker.py:stmt_{idx}", "Statement details before execute", {
                        "idx": idx,
                        "title": title,
                        "statement_preview": clean_statement[:200],
                        "statement_len": len(clean_statement),
                        "starts_with_create": clean_statement.upper().startswith("CREATE")
                    })

                    logger.info(f"Executing statement: {title if title else 'Setup/Untitled'}")
                    try:
                        rows = conn.execute_query(clean_statement)
                        _debug_log("A", f"weekly_stats_worker.py:result_{idx}", "Execute returned successfully", {
                            "idx": idx,
                            "title": title,
                            "rows_count": len(rows) if rows else 0
                        })

                        # If it's a SELECT returning data and has a title, store it
                        if title and rows:
                            results_map[title] = rows
                            logger.info(f"Captured {len(rows)} rows for '{title}'")

                    except Exception as e:
                        _debug_log("A,C", f"weekly_stats_worker.py:error_{idx}", "Exception during execute_query", {
                            "idx": idx,
                            "title": title,
                            "error": str(e),
                            "error_type": type(e).__name__
                        })
                        logger.error(f"Error executing statement '{title}': {e}")
                        raise e

        except Exception as e:
            logger.error(f"Database error: {e}")
            raise

        if not results_map:
            logger.warning("No results generated to save.")

        # Write to Redis (using synchronous Redis client from BaseWorker)
        try:
            if results_map:
                # Save as JSON string
                self.store_in_redis(redis_key, results_map, ttl=60 * 60 * 24 * 8)  # 8 days
                logger.info(f"Saved weekly stats to Redis key: {redis_key}. Size: {len(results_map)} sections.")
            else:
                logger.info("Results map empty, skipping Redis write.")

        except Exception as e:
            logger.error(f"Redis error: {e}")
            raise

        return {
            "sections_count": len(results_map),
            "redis_key": redis_key,
            "week_start": start_str,
            "week_end": end_str,
        }
