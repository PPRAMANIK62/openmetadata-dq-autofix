"""Pytest fixtures for preview tests."""

import pytest

from dq_autofix.openmetadata.models import SampleData, TestCaseResult
from dq_autofix.strategies.base import FailureContext


@pytest.fixture
def simple_sample_data() -> SampleData:
    """Simple sample data for testing."""
    return SampleData(
        table_fqn="db.schema.table",
        columns=["id", "name", "value"],
        rows=[
            [1, "Alice", 100],
            [2, "Bob", None],
            [3, "Charlie", 300],
            [4, "Diana", None],
            [5, "Eve", 500],
        ],
    )


@pytest.fixture
def simple_test_case() -> TestCaseResult:
    """Simple test case for testing."""
    return TestCaseResult(
        id="tc-001",
        name="test_not_null",
        test_definition="columnValuesToNotBeNull",
        entity_link="<#E::table::db.schema.table::columns::value>",
    )


@pytest.fixture
def simple_context(
    simple_test_case: TestCaseResult,
    simple_sample_data: SampleData,
) -> FailureContext:
    """Simple failure context for testing."""
    return FailureContext(
        test_case=simple_test_case,
        sample_data=simple_sample_data,
        table_row_count=100,
    )
