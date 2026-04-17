"""Tests for SampleFetcher."""

from unittest.mock import AsyncMock

import pytest

from dq_autofix.analyzer.sample_fetcher import SampleFetcher, SampleFetchResult
from dq_autofix.openmetadata.client import OpenMetadataClient, OpenMetadataClientError
from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TableProfile,
    TestCaseResult,
)


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
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher(sample_limit=100)
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        assert result.column_profile is not None
        assert result.column_profile.name == "email"
        assert result.table_row_count == 1000
        assert result.has_errors is False

    async def test_fetch_for_failure_no_sample_data(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        table_profile: TableProfile,
    ) -> None:
        """Test fetch when sample data is not available."""
        mock_client.get_table_sample_data.return_value = None
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data is None
        assert result.has_sample_data is False
        assert result.has_errors is True
        assert any("No sample data" in e for e in result.fetch_errors)

    async def test_fetch_for_failure_no_profile(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
    ) -> None:
        """Test fetch when table profile is not available."""
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = None

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        assert result.column_profile is None
        assert result.has_errors is True
        assert any("No profile" in e for e in result.fetch_errors)

    async def test_fetch_for_failure_sample_data_error(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        table_profile: TableProfile,
    ) -> None:
        """Test fetch when sample data API call fails."""
        mock_client.get_table_sample_data.side_effect = OpenMetadataClientError("API error")
        mock_client.get_table_profile.return_value = table_profile

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data is None
        assert result.has_errors is True
        assert any("Failed to fetch sample data" in e for e in result.fetch_errors)

    async def test_fetch_for_failure_profile_error(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
    ) -> None:
        """Test fetch when profile API call fails."""
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.side_effect = OpenMetadataClientError("Profile error")

        fetcher = SampleFetcher()
        result = await fetcher.fetch_for_failure(mock_client, test_case)

        assert result.sample_data == sample_data
        assert result.column_profile is None
        assert result.has_errors is True
        assert any("Failed to fetch table profile" in e for e in result.fetch_errors)

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
