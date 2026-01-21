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
