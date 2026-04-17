"""API endpoint tests."""

from fastapi.testclient import TestClient

from dq_autofix import __version__
from tests.conftest import requires_openmetadata


def test_root_health_check(client: TestClient):
    """Test root health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_api_health_check(client: TestClient):
    """Test API health check endpoint."""
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["version"] == __version__
    assert "openmetadata_host" in data


@requires_openmetadata
def test_list_failures(client: TestClient):
    """Test listing failed DQ tests."""
    response = client.get("/api/v1/failures")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "total" in data
    assert isinstance(data["data"], list)
    assert data["total"] >= 0


@requires_openmetadata
def test_list_failures_has_expected_fields(client: TestClient):
    """Test that failure list items have expected fields when present."""
    response = client.get("/api/v1/failures")
    assert response.status_code == 200
    data = response.json()

    if data["total"] > 0:
        failure = data["data"][0]
        assert "id" in failure
        assert "name" in failure
        assert "testDefinition" in failure
        assert "tableFqn" in failure


@requires_openmetadata
def test_get_failure_not_found(client: TestClient):
    """Test 404 response for non-existent failure."""
    response = client.get("/api/v1/failures/non-existent-id-12345")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data


def test_openapi_docs_available(client: TestClient):
    """Test that OpenAPI documentation is available."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    data = response.json()
    assert data["info"]["title"] == "DQ AutoFix"
    assert "paths" in data
