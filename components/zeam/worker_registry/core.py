"""
Worker Registry for Celery tasks.
This component is a lightweight contract shared between Scheduler and Worker.
"""

class WorkerNames:
    """
    Central repository for all Celery task names.
    Both the Scheduler and Worker must reference these constants.
    """
    CURATED_CONTENT_POPULARITY = "workers.curated_content_popularity"
    CURATED_CONTENT_POPULARITY_BY_PLATFORM = "workers.curated_content_popularity_by_platform"


WORKER_NAMES = [
    WorkerNames.CURATED_CONTENT_POPULARITY,
    WorkerNames.CURATED_CONTENT_POPULARITY_BY_PLATFORM,
]