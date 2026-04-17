"""FastAPI application factory and entry point."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dq_autofix import __version__
from dq_autofix.api.routes import router as api_router
from dq_autofix.config import get_settings
from dq_autofix.openmetadata.client import OpenMetadataClient


def setup_logging() -> None:
    """Configure logging based on settings."""
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application lifecycle.

    Initializes OpenMetadata client on startup and closes it on shutdown.
    """
    settings = get_settings()
    logger = logging.getLogger(__name__)

    logger.info(f"Starting DQ AutoFix v{__version__}")
    logger.info(f"OpenMetadata host: {settings.openmetadata_host}")

    app.state.om_client = OpenMetadataClient(settings)

    yield

    logger.info("Shutting down DQ AutoFix")
    await app.state.om_client.close()


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    setup_logging()

    app = FastAPI(
        title="DQ AutoFix",
        description=(
            "AI-powered repair suggestions for failed OpenMetadata Data Quality checks. "
            "Analyzes DQ failures, proposes fixes with confidence scores, "
            "and provides safe preview with rollback guards."
        ),
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1")

    @app.get("/health", include_in_schema=False)
    async def root_health() -> dict[str, str]:
        """Root health check endpoint."""
        return {"status": "healthy"}

    return app


app = create_app()
