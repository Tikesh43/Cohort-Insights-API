import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.db import close_motor_client, get_database, init_db
from app.logging_config import setup_logging
from app.redis_client import close_redis, get_redis_client
from app.routers import documents, health, users
from app.workers.pipeline import run_worker_loop

# Initialize structured JSON logging
setup_logging(level="DEBUG" if settings.DEBUG else "INFO")
logger = logging.getLogger(__name__)

worker_task: Optional[asyncio.Task] = None
worker_stop_event = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager:
    - Initializes MongoDB connection and creates schema indexes.
    - Warms up Redis connection pool.
    - Spawns in-process pipeline worker task if enabled.
    - Performs graceful shutdown on application exit.
    """
    logger.info("Starting up Cohort Insights API...", extra={"environment": settings.ENVIRONMENT})

    # 1. Initialize MongoDB and indexes
    db = get_database()
    await init_db(db)

    # 2. Initialize Redis client
    redis_client = get_redis_client()

    # 3. Start background pipeline worker if configured
    global worker_task, worker_stop_event
    if settings.ENABLE_IN_PROCESS_WORKER:
        worker_stop_event.clear()
        worker_task = asyncio.create_task(
            run_worker_loop(db, redis_client, worker_stop_event),
            name="pipeline_worker",
        )
        logger.info("In-process pipeline background worker task started")

    yield

    # Teardown
    logger.info("Shutting down Cohort Insights API...")
    if worker_task and not worker_task.done():
        worker_stop_event.set()
        worker_task.cancel()
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    await close_redis()
    close_motor_client()
    logger.info("Cohort Insights API shutdown complete")


app = FastAPI(
    title=settings.APP_NAME,
    description="Cohort Insights API take-home project with multi-stage simulated pipeline, "
                "strict staleness prevention, Redis rate-limiting, and content-hash caching.",
    version="1.0.0",
    lifespan=lifespan,
)

# Standard CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register endpoints
app.include_router(health.router)
app.include_router(documents.router)
app.include_router(users.router)
