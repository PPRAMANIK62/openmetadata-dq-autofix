"""OpenMetadata client tests."""

import pytest

from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.openmetadata.models import TestCaseResult
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
