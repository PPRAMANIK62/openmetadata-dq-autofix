"""Pytest fixtures and configuration."""

from collections.abc import Iterator

import pytest
from dotenv import load_dotenv
from fastapi.testclient import TestClient

from dq_autofix.config import Settings
from dq_autofix.main import create_app
from dq_autofix.openmetadata.client import OpenMetadataClient

load_dotenv()


def has_openmetadata_token() -> bool:
    """Check if OpenMetadata token is configured."""
    settings = Settings()
    return bool(settings.openmetadata_token)


requires_openmetadata = pytest.mark.skipif(
    not has_openmetadata_token(), reason="OPENMETADATA_TOKEN not configured in .env"
)


@pytest.fixture
def settings() -> Settings:
    """Create test settings."""
    return Settings(log_level="DEBUG")


@pytest.fixture
def om_client(settings: Settings) -> OpenMetadataClient:
    """Create OpenMetadata client for testing."""
    return OpenMetadataClient(settings)


@pytest.fixture
def app():
    """Create FastAPI test application."""
    return create_app()


@pytest.fixture
def client(app) -> Iterator[TestClient]:
    """Create test client for API testing with lifespan context."""
    with TestClient(app) as test_client:
        yield test_client
