from fastapi import FastAPI
from zeam.api.api.v1.recommend import router as v1_router
from zeam.api.api.health import router as health_router
from zeam.api.api.redis import router as redis_router
from zeam.api.api.scheduler import router as scheduler_router
from zeam.config.core import settings
import uvicorn

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API for Popularity Recommendations",
    version="1.0.0",
)

app.include_router(v1_router, prefix="/api/v1")
app.include_router(health_router, prefix="/api/health")
app.include_router(redis_router, prefix="/api/redis")
app.include_router(scheduler_router, prefix="/api/scheduler")

@app.get("/")
async def root():
    return {"message": "Popularity Recommender Service"}

if __name__ == "__main__":
    # Start a development server that respects SERVER_PORT environment variable
    uvicorn.run(
        "zeam.api.main:app",
        host="0.0.0.0",
        port=settings.SERVER_PORT,
        reload=True,
    )

