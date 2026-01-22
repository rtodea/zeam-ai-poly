from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from celery import Celery
import os

from zeam.worker_registry.core import WORKER_NAMES

router = APIRouter()

# Create Celery app instance to send tasks
# This doesn't import the scheduler package, just creates a client
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", "6379")
redis_db = os.getenv("REDIS_DB", "0")

celery_app = Celery(
    "scheduler",
    broker=f"redis://{redis_host}:{redis_port}/{redis_db}",
    backend=f"redis://{redis_host}:{redis_port}/{redis_db}"
)

# Map of worker names to their Celery task names
TASK_NAMES: Dict[str, str] = {

}

@router.post("/run/{worker_name}")
async def run_worker(worker_name: str, request: Dict[str, Any]):
    """
    Trigger a Celery worker task by name.
    The task runs asynchronously via Celery.
    """
    if worker_name not in WORKER_NAMES:
        raise HTTPException(
            status_code=404,
            detail=f"Worker '{worker_name}' not found. Available workers: {WORKER_NAMES}"
        )

    # Send task to Celery via the message broker
    result = celery_app.send_task(worker_name, kwargs=request)

    return {
        "message": f"Worker '{worker_name}' triggered",
        "task_id": result.id,
        "status": "pending"
    }
