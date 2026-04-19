"""OpenMetadata client tests."""

from datetime import UTC, datetime

import pytest

from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.openmetadata.models import (
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
    TestResultValue,
)
from tests.conftest import requires_openmetadata


@requires_openmetadata
@pytest.mark.asyncio
async def test_get_failed_test_cases(om_client: OpenMetadataClient):
    """Test fetching failed test cases from OpenMetadata API."""
    failures = await om_client.get_failed_test_cases()
    assert isinstance(failures, list)


@requires_openmetadata
@pytest.mark.asyncio
async def test_get_test_case_result_not_found(om_client: OpenMetadataClient):
    """Test that non-existent test case returns None."""
    result = await om_client.get_test_case_result("non-existent-test-case-12345")
    assert result is None


@pytest.mark.asyncio
async def test_table_fqn_parsing():
    """Test table FQN parsing from entity link."""
    tc = TestCaseResult(
        id="test",
        name="test",
        testDefinition="test",
        entityLink="<#E::table::sample_data.db.table::columns::col>",
    )
    assert tc.table_fqn == "sample_data.db.table"
    assert tc.column_name == "col"


@pytest.mark.asyncio
async def test_table_fqn_parsing_without_column():
    """Test table FQN parsing when no column is specified."""
    tc = TestCaseResult(
        id="test",
        name="test",
        testDefinition="test",
        entityLink="<#E::table::sample_data.db.table>",
    )
    assert tc.table_fqn == "sample_data.db.table"
    assert tc.column_name is None


@pytest.mark.asyncio
async def test_client_close(om_client: OpenMetadataClient):
    """Test that client can be closed properly."""
    await om_client.close()
    assert om_client._http_client is None


class TestGetAffectedCount:
    """Tests for TestCaseResultSummary.get_affected_count()."""

    def test_returns_failed_rows_when_present(self):
        """Test that failedRows is returned when available."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=5,
        )
        assert result.get_affected_count() == 5

    def test_extracts_null_count_from_test_result_value(self):
        """Test extraction of nullCount from testResultValue."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            test_result_value=[
                TestResultValue(name="nullCount", value="3"),
            ],
        )
        assert result.get_affected_count() == 3

    def test_computes_duplicates_from_value_and_unique_count(self):
        """Test computation of duplicates from valueCount - uniqueCount."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            test_result_value=[
                TestResultValue(name="valueCount", value="18"),
                TestResultValue(name="uniqueCount", value="14"),
            ],
        )
        assert result.get_affected_count() == 4  # 18 - 14 = 4 duplicates

    def test_prefers_failed_rows_over_test_result_value(self):
        """Test that failedRows takes precedence over testResultValue."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=10,
            test_result_value=[
                TestResultValue(name="nullCount", value="5"),
            ],
        )
        assert result.get_affected_count() == 10

    def test_returns_none_when_no_data_available(self):
        """Test returns None when no count data is available."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
        )
        assert result.get_affected_count() is None

    def test_handles_invalid_values_gracefully(self):
        """Test that invalid values don't cause errors."""
        result = TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            test_result_value=[
                TestResultValue(name="nullCount", value="not-a-number"),
            ],
        )
        assert result.get_affected_count() is None
