"""Pytest fixtures for strategy tests."""

from datetime import UTC, datetime

import pytest

from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies import FailureContext


@pytest.fixture
def null_test_case() -> TestCaseResult:
    """Test case for null value failure."""
    return TestCaseResult(
        id="tc-001",
        name="customer_id_not_null",
        test_definition="columnValuesToNotBeNull",
        entity_link="<#E::table::sample_data.ecommerce.customers::columns::customer_id>",
        result=TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=127,
            passed_rows=5373,
            failed_rows_percentage=2.3,
            passed_rows_percentage=97.7,
        ),
    )


@pytest.fixture
def numeric_column_profile() -> ColumnProfile:
    """Column profile for a numeric column with nulls."""
    return ColumnProfile(
        name="customer_id",
        data_type="INTEGER",
        null_count=127,
        null_proportion=0.023,
        unique_count=5373,
        values_count=5373,
        mean=45892.5,
        median=45000.0,
        std_dev=12500.0,
        min=1000,
        max=99999,
    )


@pytest.fixture
def sample_data_with_nulls() -> SampleData:
    """Sample data containing null values."""
    return SampleData(
        table_fqn="sample_data.ecommerce.customers",
        columns=["id", "customer_id", "name", "email"],
        rows=[
            [1, 1001, "John Doe", "john@example.com"],
            [2, None, "Jane Smith", "jane@example.com"],
            [3, 1003, "Bob Wilson", "bob@example.com"],
            [4, None, "Alice Brown", "alice@example.com"],
            [5, 1005, "Charlie Davis", "charlie@example.com"],
            [6, None, "Eve Martin", "eve@example.com"],
        ],
    )


@pytest.fixture
def null_failure_context(
    null_test_case: TestCaseResult,
    numeric_column_profile: ColumnProfile,
    sample_data_with_nulls: SampleData,
) -> FailureContext:
    """Complete failure context for null value tests."""
    return FailureContext(
        test_case=null_test_case,
        column_profile=numeric_column_profile,
        sample_data=sample_data_with_nulls,
        table_row_count=5500,
    )


@pytest.fixture
def regex_test_case() -> TestCaseResult:
    """Test case for regex match failure."""
    return TestCaseResult(
        id="tc-002",
        name="email_format_valid",
        test_definition="columnValuesToMatchRegex",
        entity_link="<#E::table::sample_data.ecommerce.customers::columns::email>",
        result=TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=89,
            passed_rows=5411,
            failed_rows_percentage=1.6,
            passed_rows_percentage=98.4,
        ),
    )


@pytest.fixture
def sample_data_with_whitespace() -> SampleData:
    """Sample data with whitespace issues."""
    return SampleData(
        table_fqn="sample_data.ecommerce.customers",
        columns=["id", "email", "name"],
        rows=[
            [1, "john@example.com", "John Doe"],
            [2, "  jane@example.com", "Jane Smith"],
            [3, "bob@example.com  ", "Bob Wilson"],
            [4, "  alice@example.com  ", "Alice Brown"],
            [5, "charlie@example.com", "Charlie Davis"],
        ],
    )


@pytest.fixture
def whitespace_failure_context(
    regex_test_case: TestCaseResult,
    sample_data_with_whitespace: SampleData,
) -> FailureContext:
    """Failure context for whitespace tests."""
    return FailureContext(
        test_case=regex_test_case,
        column_profile=None,
        sample_data=sample_data_with_whitespace,
        table_row_count=5500,
    )


@pytest.fixture
def unique_test_case() -> TestCaseResult:
    """Test case for unique value failure."""
    return TestCaseResult(
        id="tc-003",
        name="order_id_unique",
        test_definition="columnValuesToBeUnique",
        entity_link="<#E::table::sample_data.ecommerce.orders::columns::order_id>",
        result=TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=45,
            passed_rows=11955,
            failed_rows_percentage=0.4,
            passed_rows_percentage=99.6,
        ),
    )


@pytest.fixture
def sample_data_with_duplicates() -> SampleData:
    """Sample data containing duplicate values."""
    return SampleData(
        table_fqn="sample_data.ecommerce.orders",
        columns=["id", "order_id", "customer_id", "created_at"],
        rows=[
            [1, "ORD-001", 1001, "2024-01-15"],
            [2, "ORD-002", 1002, "2024-01-16"],
            [3, "ORD-001", 1003, "2024-01-17"],
            [4, "ORD-003", 1004, "2024-01-18"],
            [5, "ORD-002", 1005, "2024-01-19"],
            [6, "ORD-004", 1006, "2024-01-20"],
        ],
    )


@pytest.fixture
def duplicate_failure_context(
    unique_test_case: TestCaseResult,
    sample_data_with_duplicates: SampleData,
) -> FailureContext:
    """Failure context for deduplication tests."""
    return FailureContext(
        test_case=unique_test_case,
        column_profile=None,
        sample_data=sample_data_with_duplicates,
        table_row_count=12000,
    )


@pytest.fixture
def categorical_column_profile() -> ColumnProfile:
    """Column profile for a categorical column."""
    return ColumnProfile(
        name="status",
        data_type="VARCHAR",
        null_count=50,
        null_proportion=0.01,
        unique_count=5,
        values_count=4950,
        distinct_count=5,
    )


@pytest.fixture
def sample_data_categorical() -> SampleData:
    """Sample data with categorical values and nulls."""
    return SampleData(
        table_fqn="sample_data.ecommerce.orders",
        columns=["id", "status", "customer_id"],
        rows=[
            [1, "pending", 1001],
            [2, "pending", 1002],
            [3, None, 1003],
            [4, "shipped", 1004],
            [5, "pending", 1005],
            [6, None, 1006],
            [7, "pending", 1007],
            [8, "delivered", 1008],
        ],
    )


@pytest.fixture
def categorical_null_test_case() -> TestCaseResult:
    """Test case for categorical null value failure."""
    return TestCaseResult(
        id="tc-004",
        name="status_not_null",
        test_definition="columnValuesToNotBeNull",
        entity_link="<#E::table::sample_data.ecommerce.orders::columns::status>",
        result=TestCaseResultSummary(
            status=TestResultStatus.FAILED,
            timestamp=datetime.now(UTC),
            failed_rows=50,
            passed_rows=4950,
            failed_rows_percentage=1.0,
            passed_rows_percentage=99.0,
        ),
    )


@pytest.fixture
def categorical_failure_context(
    categorical_null_test_case: TestCaseResult,
    categorical_column_profile: ColumnProfile,
    sample_data_categorical: SampleData,
) -> FailureContext:
    """Failure context for categorical column tests."""
    return FailureContext(
        test_case=categorical_null_test_case,
        column_profile=categorical_column_profile,
        sample_data=sample_data_categorical,
        table_row_count=5000,
    )
