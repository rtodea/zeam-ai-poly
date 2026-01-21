"""
Base worker class with common functionality for all workers.
"""
import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path

import redis

logger = logging.getLogger(__name__)


class BaseWorker:
    """Base class for all workers with common utilities."""

    def __init__(self):
        self.redis_client = self._init_redis()
        self.db_config = self._get_db_config()

    def _init_redis(self) -> redis.Redis:
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

    def _get_db_config(self) -> Dict[str, Any]:
        """Get database configuration from environment."""
        return {
            "host": os.getenv("REDSHIFT_HOST", "localhost"),
            "port": int(os.getenv("REDSHIFT_PORT", "5432")),
            "user": os.getenv("REDSHIFT_USER", "user"),
            "password": os.getenv("REDSHIFT_PASSWORD", "password"),
            "database": os.getenv("REDSHIFT_DB", "db"),
            "schema": os.getenv("REDSHIFT_SCHEMA", "public"),
        }

    def load_sql_query(self, sql_file: str) -> str:
        """Load SQL query from file."""
        sql_path = Path(__file__).parent.parent / "sql" / sql_file
        with open(sql_path, "r") as f:
            return f.read()

    def store_in_redis(
        self,
        key: str,
        data: Any,
        ttl: Optional[int] = None
    ) -> None:
        """Store data in Redis with optional TTL."""
        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        prefixed_key = f"zeam-recommender:{key}"
        if ttl:
            self.redis_client.setex(prefixed_key, ttl, data)
        else:
            self.redis_client.set(prefixed_key, data)

        logger.info(f"Stored data in Redis: {prefixed_key}")

    def get_from_redis(self, key: str) -> Optional[Any]:
        """Get data from Redis."""
        data = self.redis_client.get(key)
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return data
        return None

    def store_metadata(
        self,
        run_id: str,
        task_name: str,
        status: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Store job run metadata in Redis."""
        key = f"job_run:{task_name}:{run_id}"
        data = {
            "run_id": run_id,
            "task_name": task_name,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            **metadata
        }
        self.store_in_redis(key, data, ttl=86400 * 30)  # 30 days

        # Also store the latest run
        latest_key = f"job_run:{task_name}:latest"
        self.store_in_redis(latest_key, data, ttl=86400 * 30)
