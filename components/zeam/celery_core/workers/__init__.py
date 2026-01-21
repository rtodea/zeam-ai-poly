"""
Workers module - contains all task workers.
"""
from zeam.scheduler.workers.base_worker import BaseWorker

from zeam.scheduler.workers.curated_content_worker import curated_content_popularity

__all__ = [
    "BaseWorker",

    "curated_content_popularity"
]
