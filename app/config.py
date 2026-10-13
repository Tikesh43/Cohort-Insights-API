import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "Cohort Insights API"
    ENVIRONMENT: str = "development"
    DEBUG: bool = False

    # MongoDB Configuration
    MONGODB_URL: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "cohort_insights"
    TEST_DATABASE_NAME: str = "cohort_insights_test"

    # Redis Configuration
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CACHE_TTL_SECONDS: int = 86400  # 24 hours
    REDIS_RATELIMIT_TTL_SECONDS: int = 3600  # 1 hour safety net

    # Concurrency and Rate Limiting
    MAX_ACTIVE_DOCUMENTS: int = 3
    MAX_CONTENT_LENGTH: int = 500_000

    # Pipeline Simulation Configuration
    PROCESSING_SECONDS: float = 15.0
    ENRICHING_SECONDS: float = 10.0
    FAILURE_RATE: float = 0.10

    # Backwards compatibility aliases
    SIMULATION_PROCESSING_DELAY_MIN: float = 10.0
    SIMULATION_PROCESSING_DELAY_MAX: float = 20.0
    SIMULATION_ENRICHING_DELAY_MIN: float = 5.0
    SIMULATION_ENRICHING_DELAY_MAX: float = 15.0
    STAGE_FAILURE_RATE: float = 0.10

    LEASE_TIMEOUT_SECONDS: int = 60
    WORKER_POLL_INTERVAL_SECONDS: float = 1.0
    ENABLE_IN_PROCESS_WORKER: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
