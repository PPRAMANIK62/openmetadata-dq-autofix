"""Integration tests with real OpenMetadata data.

These tests require:
1. OpenMetadata running at localhost:8585
2. OPENMETADATA_TOKEN set in .env
3. test_dq database with customers/orders tables
4. Failed DQ test cases created in OpenMetadata

Run with: pytest tests/test_integration.py -v
"""

import pytest
from fastapi.testclient import TestClient

from dq_autofix.config import Settings
from dq_autofix.main import create_app
from dq_autofix.openmetadata.client import OpenMetadataClient


def has_openmetadata_connection() -> bool:
    """Check if OpenMetadata is accessible."""
    import httpx
    settings = Settings()
    if not settings.openmetadata_token:
        return False
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.get(
                f"{settings.openmetadata_host}/api/v1/system/version",
                headers={"Authorization": f"Bearer {settings.openmetadata_token}"}
            )
            return response.status_code == 200
    except Exception:
        return False


requires_openmetadata = pytest.mark.skipif(
    not has_openmetadata_connection(),
    reason="OpenMetadata not accessible or token not configured"
)


@pytest.fixture
def app():
    """Create FastAPI test application."""
    return create_app()


@pytest.fixture
def client(app):
    """Create test client."""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def settings():
    """Create settings."""
    return Settings()


@pytest.fixture
def om_client(settings):
    """Create OpenMetadata client."""
    return OpenMetadataClient(settings)


# =============================================================================
# API Health Tests
# =============================================================================

class TestAPIHealth:
    """Test API health endpoints."""

    def test_root_health(self, client):
        """Test root health endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_api_health(self, client):
        """Test API health endpoint with version."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        assert "openmetadata_host" in data


# =============================================================================
# Failures Endpoint Tests
# =============================================================================

@requires_openmetadata
class TestFailuresEndpoint:
    """Test failures listing from OpenMetadata."""

    def test_list_failures(self, client):
        """Test listing failed test cases."""
        response = client.get("/api/v1/failures")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total" in data
        assert isinstance(data["data"], list)

    def test_failures_have_required_fields(self, client):
        """Test that failures have all required fields."""
        response = client.get("/api/v1/failures")
        assert response.status_code == 200
        data = response.json()
        
        if data["total"] > 0:
            failure = data["data"][0]
            assert "id" in failure
            assert "name" in failure
            assert "testDefinition" in failure
            assert "tableFqn" in failure


# =============================================================================
# Analyze Endpoint Tests
# =============================================================================

@requires_openmetadata
class TestAnalyzeEndpoint:
    """Test analysis of DQ failures."""

    def test_analyze_not_null_failure(self, client):
        """Test analyzing a columnValuesToNotBeNull failure."""
        # First get a failure
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        # Find a not-null failure
        not_null_failure = next(
            (f for f in failures if "not_null" in f["name"].lower()),
            None
        )
        
        if not_null_failure:
            response = client.post(
                "/api/v1/analyze",
                json={"testCaseId": not_null_failure["name"]}
            )
            assert response.status_code == 200
            data = response.json()
            assert "testCaseId" in data
            assert "testCaseName" in data
            assert "recommendations" in data
            assert "metadata" in data
            assert data["metadata"]["testType"] == "columnValuesToNotBeNull"

    def test_analyze_unique_failure(self, client):
        """Test analyzing a columnValuesToBeUnique failure."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        unique_failure = next(
            (f for f in failures if "unique" in f["name"].lower()),
            None
        )
        
        if unique_failure:
            response = client.post(
                "/api/v1/analyze",
                json={"testCaseId": unique_failure["name"]}
            )
            assert response.status_code == 200
            data = response.json()
            assert data["metadata"]["testType"] == "columnValuesToBeUnique"

    def test_analyze_regex_failure(self, client):
        """Test analyzing a columnValuesToMatchRegex failure."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        regex_failure = next(
            (f for f in failures if "regex" in f["name"].lower()),
            None
        )
        
        if regex_failure:
            response = client.post(
                "/api/v1/analyze",
                json={"testCaseId": regex_failure["name"]}
            )
            assert response.status_code == 200
            data = response.json()
            assert data["metadata"]["testType"] == "columnValuesToMatchRegex"

    def test_analyze_nonexistent_failure(self, client):
        """Test analyzing a non-existent test case."""
        response = client.post(
            "/api/v1/analyze",
            json={"testCaseId": "nonexistent_test_case_xyz123"}
        )
        assert response.status_code == 404

    def test_analyze_missing_parameters(self, client):
        """Test analyze with missing parameters."""
        response = client.post("/api/v1/analyze", json={})
        assert response.status_code == 400


# =============================================================================
# Suggest Endpoint Tests
# =============================================================================

@requires_openmetadata
class TestSuggestEndpoint:
    """Test fix suggestions."""

    def test_suggest_returns_fix_sql(self, client):
        """Test that suggest returns fix SQL."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        if failures:
            response = client.post(
                "/api/v1/suggest",
                json={"failureId": failures[0]["name"]}
            )
            # May return 404 if no strategy applies, or 200 with suggestion
            if response.status_code == 200:
                data = response.json()
                assert "strategy" in data
                assert "fixSql" in data
                assert "rollbackSql" in data
                assert "confidenceScore" in data

    def test_suggest_with_strategy_override(self, client):
        """Test suggest with explicit strategy override."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        # Find a unique failure for keep_first strategy
        unique_failure = next(
            (f for f in failures if "unique" in f["name"].lower()),
            None
        )
        
        if unique_failure:
            response = client.post(
                "/api/v1/suggest",
                json={
                    "failureId": unique_failure["name"],
                    "strategyOverride": "keep_first"
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert data["strategy"] == "keep_first"
            assert "DELETE FROM" in data["fixSql"]

    def test_suggest_trim_whitespace(self, client):
        """Test trim_whitespace strategy."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        regex_failure = next(
            (f for f in failures if "regex" in f["name"].lower()),
            None
        )
        
        if regex_failure:
            response = client.post(
                "/api/v1/suggest",
                json={
                    "failureId": regex_failure["name"],
                    "strategyOverride": "trim_whitespace"
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert data["strategy"] == "trim_whitespace"
            assert "TRIM" in data["fixSql"]

    def test_suggest_invalid_strategy(self, client):
        """Test suggest with invalid strategy."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        if failures:
            response = client.post(
                "/api/v1/suggest",
                json={
                    "failureId": failures[0]["name"],
                    "strategyOverride": "invalid_strategy_xyz"
                }
            )
            assert response.status_code == 404


# =============================================================================
# Preview Endpoint Tests
# =============================================================================

@requires_openmetadata
class TestPreviewEndpoint:
    """Test preview functionality."""

    def test_preview_shows_before_after(self, client):
        """Test that preview shows before/after samples."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        regex_failure = next(
            (f for f in failures if "regex" in f["name"].lower()),
            None
        )
        
        if regex_failure:
            response = client.post(
                "/api/v1/preview",
                json={
                    "failureId": regex_failure["name"],
                    "strategy": "trim_whitespace"
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert "preview" in data
            preview = data["preview"]
            assert "beforeSample" in preview
            assert "afterSample" in preview
            assert "changesSummary" in preview


# =============================================================================
# Strategies Endpoint Tests
# =============================================================================

class TestStrategiesEndpoint:
    """Test strategies listing."""

    def test_list_strategies(self, client):
        """Test listing all available strategies."""
        response = client.get("/api/v1/strategies")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total" in data
        assert data["total"] == 8  # 8 strategies registered

    def test_strategies_have_required_fields(self, client):
        """Test that strategies have all required fields."""
        response = client.get("/api/v1/strategies")
        data = response.json()
        
        for strategy in data["data"]:
            assert "name" in strategy
            assert "description" in strategy
            assert "supportedTestTypes" in strategy
            assert "reversibilityScore" in strategy

    def test_expected_strategies_exist(self, client):
        """Test that all expected strategies are registered."""
        response = client.get("/api/v1/strategies")
        data = response.json()
        
        strategy_names = {s["name"] for s in data["data"]}
        expected = {
            "mean_imputation",
            "median_imputation", 
            "mode_imputation",
            "forward_fill",
            "trim_whitespace",
            "normalize_case",
            "keep_first",
            "keep_last",
        }
        assert expected == strategy_names


# =============================================================================
# OpenMetadata Client Tests
# =============================================================================

@requires_openmetadata
class TestOpenMetadataClient:
    """Test OpenMetadata client directly."""

    @pytest.mark.asyncio
    async def test_get_failed_test_cases(self, om_client):
        """Test fetching failed test cases."""
        try:
            failures = await om_client.get_failed_test_cases()
            assert isinstance(failures, list)
        finally:
            await om_client.close()

    @pytest.mark.asyncio
    async def test_get_test_case_result(self, om_client):
        """Test fetching a specific test case."""
        try:
            failures = await om_client.get_failed_test_cases()
            if failures:
                result = await om_client.get_test_case_result(failures[0].name)
                assert result is not None
                assert result.name == failures[0].name
        finally:
            await om_client.close()

    @pytest.mark.asyncio
    async def test_get_sample_data(self, om_client):
        """Test fetching sample data."""
        try:
            sample = await om_client.get_table_sample_data(
                "test_mysql.default.test_dq.customers"
            )
            if sample:
                assert sample.columns is not None
                assert sample.rows is not None
                assert len(sample.rows) > 0
        finally:
            await om_client.close()


# =============================================================================
# End-to-End Flow Tests
# =============================================================================

@requires_openmetadata
class TestEndToEndFlow:
    """Test complete end-to-end workflows."""

    def test_full_flow_null_check(self, client):
        """Test full flow: list failures → analyze → suggest for null check."""
        # Step 1: List failures
        failures_response = client.get("/api/v1/failures")
        assert failures_response.status_code == 200
        failures = failures_response.json()["data"]
        
        not_null_failure = next(
            (f for f in failures if "not_null" in f["name"].lower()),
            None
        )
        if not not_null_failure:
            pytest.skip("No not-null failure found")
        
        # Step 2: Analyze
        analyze_response = client.post(
            "/api/v1/analyze",
            json={"testCaseId": not_null_failure["name"]}
        )
        assert analyze_response.status_code == 200
        analysis = analyze_response.json()
        assert analysis["metadata"]["testType"] == "columnValuesToNotBeNull"
        
        # Step 3: Suggest
        suggest_response = client.post(
            "/api/v1/suggest",
            json={"failureId": not_null_failure["name"]}
        )
        if suggest_response.status_code == 200:
            suggestion = suggest_response.json()
            assert suggestion["fixSql"] is not None
            assert suggestion["rollbackSql"] is not None

    def test_full_flow_duplicate_check(self, client):
        """Test full flow for duplicate/uniqueness check."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        unique_failure = next(
            (f for f in failures if "unique" in f["name"].lower()),
            None
        )
        if not unique_failure:
            pytest.skip("No unique failure found")
        
        # Analyze
        analyze_response = client.post(
            "/api/v1/analyze",
            json={"testCaseId": unique_failure["name"]}
        )
        assert analyze_response.status_code == 200
        
        # Suggest with keep_first
        suggest_response = client.post(
            "/api/v1/suggest",
            json={
                "failureId": unique_failure["name"],
                "strategyOverride": "keep_first"
            }
        )
        assert suggest_response.status_code == 200
        suggestion = suggest_response.json()
        assert "DELETE FROM" in suggestion["fixSql"]
        assert suggestion["strategy"] == "keep_first"

    def test_full_flow_whitespace_check(self, client):
        """Test full flow for whitespace/regex check."""
        failures_response = client.get("/api/v1/failures")
        failures = failures_response.json()["data"]
        
        regex_failure = next(
            (f for f in failures if "regex" in f["name"].lower()),
            None
        )
        if not regex_failure:
            pytest.skip("No regex failure found")
        
        # Analyze
        analyze_response = client.post(
            "/api/v1/analyze",
            json={"testCaseId": regex_failure["name"]}
        )
        assert analyze_response.status_code == 200
        analysis = analyze_response.json()
        
        # Should recommend trim_whitespace with high confidence
        if analysis["recommendations"]:
            best = analysis["bestStrategy"]
            if best:
                assert best["confidenceScore"] > 0.5
        
        # Suggest
        suggest_response = client.post(
            "/api/v1/suggest",
            json={"failureId": regex_failure["name"]}
        )
        if suggest_response.status_code == 200:
            suggestion = suggest_response.json()
            assert "TRIM" in suggestion["fixSql"]
            # Check preview has before/after
            assert suggestion["preview"]["beforeSample"] is not None


# =============================================================================
# SQL Generation Tests
# =============================================================================

@requires_openmetadata
class TestSQLGeneration:
    """Test SQL generation for different strategies."""

    def test_trim_whitespace_sql(self, client):
        """Test trim_whitespace generates valid SQL."""
        failures = client.get("/api/v1/failures").json()["data"]
        regex_failure = next(
            (f for f in failures if "regex" in f["name"].lower()),
            None
        )
        
        if regex_failure:
            response = client.post(
                "/api/v1/suggest",
                json={
                    "failureId": regex_failure["name"],
                    "strategyOverride": "trim_whitespace"
                }
            )
            if response.status_code == 200:
                sql = response.json()["fixSql"]
                assert "UPDATE" in sql
                assert "TRIM" in sql
                assert "WHERE" in sql

    def test_keep_first_sql(self, client):
        """Test keep_first generates valid DELETE SQL."""
        failures = client.get("/api/v1/failures").json()["data"]
        unique_failure = next(
            (f for f in failures if "unique" in f["name"].lower()),
            None
        )
        
        if unique_failure:
            response = client.post(
                "/api/v1/suggest",
                json={
                    "failureId": unique_failure["name"],
                    "strategyOverride": "keep_first"
                }
            )
            if response.status_code == 200:
                sql = response.json()["fixSql"]
                assert "DELETE FROM" in sql
                assert "MIN" in sql  # keep_first uses MIN

    def test_rollback_sql_exists(self, client):
        """Test that rollback SQL is always generated."""
        failures = client.get("/api/v1/failures").json()["data"]
        
        for failure in failures[:3]:  # Test first 3
            response = client.post(
                "/api/v1/suggest",
                json={"failureId": failure["name"]}
            )
            if response.status_code == 200:
                data = response.json()
                assert data["rollbackSql"] is not None
                assert len(data["rollbackSql"]) > 0
