"""Sample data fetcher for DQ failure analysis."""

import logging
from dataclasses import dataclass, field

from dq_autofix.openmetadata.client import OpenMetadataClient, OpenMetadataClientError
from dq_autofix.openmetadata.models import ColumnProfile, SampleData, TestCaseResult

logger = logging.getLogger(__name__)


@dataclass
class SampleFetchResult:
    """Result of fetching sample data and profile for a failure.

    Contains the sample data rows, column profile statistics,
    and any errors encountered during fetching.
    """

    sample_data: SampleData | None = None
    column_profile: ColumnProfile | None = None
    table_row_count: int | None = None
    fetch_errors: list[str] = field(default_factory=list)

    @property
    def has_sample_data(self) -> bool:
        """Check if sample data was successfully fetched."""
        return self.sample_data is not None and len(self.sample_data.rows) > 0

    @property
    def has_profile(self) -> bool:
        """Check if column profile was successfully fetched."""
        return self.column_profile is not None

    @property
    def has_errors(self) -> bool:
        """Check if any errors occurred during fetching."""
        return len(self.fetch_errors) > 0


class SampleFetcher:
    """Fetches sample data and profiles from OpenMetadata for analysis.

    Retrieves the data needed by the pattern detector and strategies
    to analyze failures and calculate confidence scores.
    """

    def __init__(self, sample_limit: int = 100) -> None:
        """Initialize the sample fetcher.

        Args:
            sample_limit: Maximum number of sample rows to fetch.
        """
        self.sample_limit = sample_limit

    async def fetch_for_failure(
        self,
        client: OpenMetadataClient,
        test_case: TestCaseResult,
    ) -> SampleFetchResult:
        """Fetch sample data and profile for a test case failure.

        Retrieves table sample data and column profile (if applicable)
        for the table/column referenced by the test case.

        Args:
            client: OpenMetadata API client.
            test_case: The failed test case to fetch data for.

        Returns:
            SampleFetchResult with fetched data and any errors.
        """
        result = SampleFetchResult()
        table_fqn = test_case.table_fqn
        column_name = test_case.column_name

        result.sample_data = await self._fetch_sample_data(client, table_fqn, result)

        profile_result = await self._fetch_table_profile(client, table_fqn, column_name, result)
        if profile_result:
            result.column_profile = profile_result[0]
            result.table_row_count = profile_result[1]

        return result

    async def _fetch_sample_data(
        self,
        client: OpenMetadataClient,
        table_fqn: str,
        result: SampleFetchResult,
    ) -> SampleData | None:
        """Fetch sample data for a table.

        Args:
            client: OpenMetadata API client.
            table_fqn: Fully qualified table name.
            result: Result object to append errors to.

        Returns:
            SampleData if successful, None otherwise.
        """
        try:
            sample_data = await client.get_table_sample_data(table_fqn, self.sample_limit)
            if sample_data is None:
                result.fetch_errors.append(f"No sample data available for table {table_fqn}")
            return sample_data
        except OpenMetadataClientError as e:
            error_msg = f"Failed to fetch sample data: {e}"
            logger.warning(error_msg)
            result.fetch_errors.append(error_msg)
            return None

    async def _fetch_table_profile(
        self,
        client: OpenMetadataClient,
        table_fqn: str,
        column_name: str | None,
        result: SampleFetchResult,
    ) -> tuple[ColumnProfile | None, int | None] | None:
        """Fetch table profile and extract column profile.

        Args:
            client: OpenMetadata API client.
            table_fqn: Fully qualified table name.
            column_name: Column name to extract profile for (optional).
            result: Result object to append errors to.

        Returns:
            Tuple of (ColumnProfile, row_count) if successful, None otherwise.
        """
        try:
            table_profile = await client.get_table_profile(table_fqn)
            if table_profile is None:
                result.fetch_errors.append(f"No profile available for table {table_fqn}")
                return None

            row_count = table_profile.row_count
            column_profile = None

            if column_name:
                column_profile = table_profile.get_column(column_name)
                if column_profile is None:
                    result.fetch_errors.append(f"No profile available for column {column_name}")

            return column_profile, row_count

        except OpenMetadataClientError as e:
            error_msg = f"Failed to fetch table profile: {e}"
            logger.warning(error_msg)
            result.fetch_errors.append(error_msg)
            return None

    async def fetch_sample_data_only(
        self,
        client: OpenMetadataClient,
        table_fqn: str,
    ) -> SampleData | None:
        """Fetch only sample data for a table (no profile).

        Convenience method when only sample data is needed.

        Args:
            client: OpenMetadata API client.
            table_fqn: Fully qualified table name.

        Returns:
            SampleData if successful, None otherwise.
        """
        try:
            return await client.get_table_sample_data(table_fqn, self.sample_limit)
        except OpenMetadataClientError as e:
            logger.warning(f"Failed to fetch sample data for {table_fqn}: {e}")
            return None
