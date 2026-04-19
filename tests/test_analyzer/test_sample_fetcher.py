"""Tests for SampleFetcher."""

from unittest.mock import AsyncMock

import pytest

from dq_autofix.analyzer.sample_fetcher import (
    SampleFetcher,
    SampleFetchResult,
    compute_column_stats_from_sample,
)
from dq_autofix.openmetadata.client import OpenMetadataClient, OpenMetadataClientError
from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TableProfile,
    TestCaseResult,
)


class TestComputeColumnStatsFromSample:
    """Tests for compute_column_stats_from_sample function."""

    def test_compute_stats_numeric_column(self) -> None:
        """Test computing statistics for a numeric column."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "value"],
            rows=[
                [1, 10.0],
                [2, 20.0],
                [3, 30.0],
                [4, 40.0],
                [5, 50.0],
            ],
        )

        result = compute_column_stats_from_sample(sample, "value")

        assert result is not None
        assert result.name == "value"
        assert result.mean == 30.0
        assert result.median == 30.0
        assert result.min == 10.0
        assert result.max == 50.0
        assert result.std_dev is not None
        assert result.null_count == 0
        assert result.values_count == 5
        assert result.distinct_count == 5

    def test_compute_stats_with_nulls(self) -> None:
        """Test computing statistics with null values."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "value"],
            rows=[
                [1, 10.0],
                [2, None],
                [3, 30.0],
                [4, None],
                [5, 50.0],
            ],
        )

        result = compute_column_stats_from_sample(sample, "value")

        assert result is not None
        assert result.null_count == 2
        assert result.null_proportion == 0.4
        assert result.mean == 30.0  # (10 + 30 + 50) / 3
        assert result.values_count == 3

    def test_compute_stats_string_column(self) -> None:
        """Test computing statistics for a string column (non-numeric)."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "name"],
            rows=[
                [1, "Alice"],
                [2, "Bob"],
                [3, "Charlie"],
                [4, "Alice"],  # Duplicate
                [5, None],
            ],
        )

        result = compute_column_stats_from_sample(sample, "name")

        assert result is not None
        assert result.name == "name"
        assert result.mean is None  # Not numeric
        assert result.median is None
        assert result.null_count == 1
        assert result.distinct_count == 3  # Alice, Bob, Charlie

    def test_compute_stats_column_not_found(self) -> None:
        """Test when column doesn't exist in sample data."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "name"],
            rows=[[1, "Alice"]],
        )

        result = compute_column_stats_from_sample(sample, "nonexistent")

        assert result is None

    def test_compute_stats_empty_rows(self) -> None:
        """Test with empty rows."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "value"],
            rows=[],
        )

        result = compute_column_stats_from_sample(sample, "value")

        assert result is None

    def test_compute_stats_single_value(self) -> None:
        """Test with a single value (stddev should be None)."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["value"],
            rows=[[42.0]],
        )

        result = compute_column_stats_from_sample(sample, "value")

        assert result is not None
        assert result.mean == 42.0
        assert result.median == 42.0
        assert result.std_dev is None  # Can't compute stddev with 1 value

    def test_compute_stats_integer_values(self) -> None:
        """Test with integer values (should convert to float)."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["count"],
            rows=[[1], [2], [3], [4], [5]],
        )

        result = compute_column_stats_from_sample(sample, "count")

        assert result is not None
        assert result.mean == 3.0
        assert result.median == 3.0


class TestSampleFetchResult:
    """Tests for SampleFetchResult dataclass."""

    def test_has_sample_data_true(self) -> None:
        """Test has_sample_data returns True when data exists."""
        sample = SampleData(table_fqn="db.schema.table", columns=["a"], rows=[[1]])
        result = SampleFetchResult(sample_data=sample)
        assert result.has_sample_data is True

    def test_has_sample_data_false_when_none(self) -> None:
        """Test has_sample_data returns False when None."""
        result = SampleFetchResult()
        assert result.has_sample_data is False

    def test_has_sample_data_false_when_empty_rows(self) -> None:
        """Test has_sample_data returns False when rows are empty."""
        sample = SampleData(table_fqn="db.schema.table", columns=["a"], rows=[])
        result = SampleFetchResult(sample_data=sample)
        assert result.has_sample_data is False

    def test_has_profile_true(self) -> None:
        """Test has_profile returns True when profile exists."""
        profile = ColumnProfile(name="col")
        result = SampleFetchResult(column_profile=profile)
        assert result.has_profile is True

    def test_has_profile_false(self) -> None:
        """Test has_profile returns False when None."""
        result = SampleFetchResult()
        assert result.has_profile is False

    def test_has_errors_true(self) -> None:
        """Test has_errors returns True when errors exist."""
        result = SampleFetchResult(fetch_errors=["Error 1"])
        assert result.has_errors is True

    def test_has_errors_false(self) -> None:
        """Test has_errors returns False when no errors."""
        result = SampleFetchResult()
        assert result.has_errors is False


class TestSampleFetcher:
    """Tests for SampleFetcher class."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock OpenMetadata client."""
        return AsyncMock(spec=OpenMetadataClient)

    @pytest.fixture
    def test_case(self) -> TestCaseResult:
        """Create a test case fixture."""
        return TestCaseResult(
            id="tc-001",
            name="test_nulls",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.customers::columns::email>",
        )

    @pytest.fixture
    def sample_data(self) -> SampleData:
        """Create sample data fixture."""
        return SampleData(
            table_fqn="db.schema.customers",
            columns=["id", "email", "name"],
            rows=[
                [1, "test@example.com", "Test User"],
                [2, None, "Another User"],
                [3, "  spaces@test.com  ", "Spaces"],
            ],
        )

    @pytest.fixture
    def table_profile(self) -> TableProfile:
        """Create table profile fixture."""
        from datetime import UTC, datetime

        return TableProfile(
            table_fqn="db.schema.customers",
            timestamp=datetime.now(UTC),
            row_count=1000,
            columns=[
                ColumnProfile(
                    name="email",
                    data_type="VARCHAR",
                    null_count=50,
                    null_proportion=0.05,
                ),
                ColumnProfile(
                    name="id",
                    data_type="INTEGER",
                    null_count=0,
                    null_proportion=0.0,
                ),
            ],
        )

    async def test_fetch_for_failure_success(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test successful fetch of sample data and profile."""
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_column_profiles.return_value = None  # columnProfile endpoint returns None
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher(sample_limit=100)
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        assert result.column_profile is not None
        assert result.column_profile.name == "email"
        assert result.table_row_count == 1000
        assert result.has_errors is False

    async def test_fetch_for_failure_with_column_profiles_endpoint(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
    ) -> None:
        """Test fetch using the dedicated columnProfile endpoint."""
        email_profile = ColumnProfile(
            name="email",
            data_type="VARCHAR",
            null_count=50,
            mean=None,
            median=None,
        )
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_column_profiles.return_value = {"email": email_profile}
        mock_client.get_table_profile.return_value = None

        fetcher = SampleFetcher(sample_limit=100)
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        assert result.column_profile is not None
        assert result.column_profile.name == "email"
        # Should have computed stats from sample data since profile lacked mean/median
        mock_client.get_column_profiles.assert_called_once()

    async def test_fetch_for_failure_no_sample_data(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        table_profile: TableProfile,
    ) -> None:
        """Test fetch when sample data is not available."""
        mock_client.get_table_sample_data.return_value = None
        mock_client.get_column_profiles.return_value = None
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data is None
        assert result.has_sample_data is False
        assert result.has_errors is True
        assert any("No sample data" in e for e in result.fetch_errors)

    async def test_fetch_for_failure_no_profile_computes_from_sample(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
    ) -> None:
        """Test fetch computes profile from sample when no profile available."""
        # Sample data with numeric email (for testing - normally email isn't numeric)
        sample_with_numeric = SampleData(
            table_fqn="db.schema.customers",
            columns=["id", "email", "score"],
            rows=[
                [1, "test@example.com", 10],
                [2, None, 20],
                [3, "other@test.com", 30],
            ],
        )
        mock_client.get_table_sample_data.return_value = sample_with_numeric
        mock_client.get_column_profiles.return_value = None
        mock_client.get_table_profile.return_value = None

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_with_numeric
        # Should have computed profile from sample data
        assert result.column_profile is not None
        assert result.column_profile.name == "email"
        assert result.column_profile.null_count == 1

    async def test_fetch_for_failure_sample_data_error(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        table_profile: TableProfile,
    ) -> None:
        """Test fetch when sample data API call fails."""
        mock_client.get_table_sample_data.side_effect = OpenMetadataClientError("API error")
        mock_client.get_column_profiles.return_value = None
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data is None
        assert result.has_errors is True
        assert any("Failed to fetch sample data" in e for e in result.fetch_errors)

    async def test_fetch_for_failure_profile_error_falls_back_to_sample(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
    ) -> None:
        """Test fetch falls back to sample data when profile API calls fail."""
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_column_profiles.side_effect = Exception("Column profile error")
        mock_client.get_table_profile.side_effect = OpenMetadataClientError("Profile error")

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        # Should compute profile from sample data as fallback
        assert result.column_profile is not None
        assert result.column_profile.name == "email"

    async def test_fetch_sample_data_only(
        self,
        mock_client: AsyncMock,
        sample_data: SampleData,
    ) -> None:
        """Test fetching only sample data."""
        mock_client.get_table_sample_data.return_value = sample_data

        fetcher = SampleFetcher()
        result = await fetcher.fetch_sample_data_only(mock_client, "db.schema.table")

        assert result == sample_data
        mock_client.get_table_sample_data.assert_called_once_with("db.schema.table", 100)

    async def test_fetch_sample_data_only_error(
        self,
        mock_client: AsyncMock,
    ) -> None:
        """Test fetch_sample_data_only when API fails."""
        mock_client.get_table_sample_data.side_effect = OpenMetadataClientError("Error")

        fetcher = SampleFetcher()
        result = await fetcher.fetch_sample_data_only(mock_client, "db.schema.table")

        assert result is None

    def test_custom_sample_limit(self) -> None:
        """Test fetcher uses custom sample limit."""
        fetcher = SampleFetcher(sample_limit=50)
        assert fetcher.sample_limit == 50
