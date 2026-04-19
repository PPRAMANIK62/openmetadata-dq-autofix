"""Sample data fetcher for DQ failure analysis."""

import contextlib
import logging
import statistics
from dataclasses import dataclass, field
from typing import Any

from dq_autofix.openmetadata.client import OpenMetadataClient, OpenMetadataClientError
from dq_autofix.openmetadata.models import ColumnProfile, SampleData, TestCaseResult

logger = logging.getLogger(__name__)


def compute_column_stats_from_sample(
    sample_data: SampleData,
    column_name: str,
) -> ColumnProfile | None:
    """Compute column statistics from sample data as a fallback.

    When the OpenMetadata profiler hasn't run or column profile data is unavailable,
    this function computes basic statistics (mean, median, stddev) from the sample
    data rows.

    Args:
        sample_data: Sample data containing rows from the table.
        column_name: Name of the column to compute statistics for.

    Returns:
        A ColumnProfile with computed statistics, or None if computation fails.
    """
    if column_name not in sample_data.columns:
        logger.debug(f"Column {column_name} not found in sample data columns")
        return None

    col_idx = sample_data.columns.index(column_name)

    # Extract values from sample rows
    all_values: list[Any] = []
    numeric_values: list[float] = []
    null_count = 0

    for row in sample_data.rows:
        val = row[col_idx]
        all_values.append(val)

        if val is None:
            null_count += 1
        else:
            with contextlib.suppress(ValueError, TypeError):
                numeric_values.append(float(val))

    total_count = len(all_values)
    if total_count == 0:
        return None

    # Compute basic statistics
    mean_val: float | None = None
    median_val: float | None = None
    std_dev_val: float | None = None
    min_val: float | None = None
    max_val: float | None = None

    if len(numeric_values) >= 1:
        mean_val = statistics.mean(numeric_values)
        median_val = statistics.median(numeric_values)
        min_val = min(numeric_values)
        max_val = max(numeric_values)

        if len(numeric_values) >= 2:
            std_dev_val = statistics.stdev(numeric_values)

    # Compute distinct count for all values (including non-numeric)
    non_null_values = [v for v in all_values if v is not None]
    distinct_count = len({str(v) for v in non_null_values})

    # Create a synthetic ColumnProfile (use camelCase aliases for Pydantic)
    return ColumnProfile(
        name=column_name,
        mean=mean_val,
        median=median_val,
        stddev=std_dev_val,
        min=min_val,
        max=max_val,
        nullCount=null_count,
        nullProportion=null_count / total_count if total_count > 0 else None,
        valuesCount=total_count - null_count,
        distinctCount=distinct_count,
    )


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

        Uses a multi-step approach to get column statistics:
        1. Try the dedicated /columnProfile endpoint (OpenMetadata 1.6+)
        2. Fall back to /tableProfile/latest endpoint
        3. If profile data lacks statistics, compute from sample data

        Args:
            client: OpenMetadata API client.
            table_fqn: Fully qualified table name.
            column_name: Column name to extract profile for (optional).
            result: Result object to append errors to.

        Returns:
            Tuple of (ColumnProfile, row_count) if successful, None otherwise.
        """
        column_profile: ColumnProfile | None = None
        row_count: int | None = None

        # Step 1: Try dedicated columnProfile endpoint (preferred in OM 1.6+)
        if column_name:
            try:
                column_profiles = await client.get_column_profiles(table_fqn)
                if column_profiles and column_name in column_profiles:
                    column_profile = column_profiles[column_name]
                    logger.debug(
                        f"Got column profile from /columnProfile endpoint for {column_name}"
                    )
            except Exception as e:
                logger.debug(f"columnProfile endpoint failed: {e}")

        # Step 2: Fall back to tableProfile/latest endpoint
        if column_profile is None or row_count is None:
            try:
                table_profile = await client.get_table_profile(table_fqn)
                if table_profile is not None:
                    row_count = table_profile.row_count
                    if column_name and column_profile is None:
                        column_profile = table_profile.get_column(column_name)
                        if column_profile:
                            logger.debug(
                                f"Got column profile from /tableProfile endpoint for {column_name}"
                            )
            except OpenMetadataClientError as e:
                logger.debug(f"tableProfile endpoint failed: {e}")

        # Step 3: Check if profile has statistics, otherwise compute from sample
        if column_name and column_profile is not None:
            needs_stats = (
                column_profile.mean is None
                and column_profile.median is None
                and column_profile.std_dev is None
            )
            if needs_stats and result.sample_data is not None:
                logger.info(
                    f"Column profile for {column_name} lacks statistics, computing from sample data"
                )
                computed_profile = compute_column_stats_from_sample(result.sample_data, column_name)
                if computed_profile:
                    # Merge computed stats into existing profile (use camelCase aliases)
                    column_profile = ColumnProfile(
                        name=column_profile.name,
                        dataType=column_profile.data_type,
                        nullCount=column_profile.null_count or computed_profile.null_count,
                        nullProportion=column_profile.null_proportion
                        or computed_profile.null_proportion,
                        uniqueCount=column_profile.unique_count or computed_profile.unique_count,
                        distinctCount=column_profile.distinct_count
                        or computed_profile.distinct_count,
                        mean=computed_profile.mean,
                        median=computed_profile.median,
                        stddev=computed_profile.std_dev,
                        min=computed_profile.min,
                        max=computed_profile.max,
                        valuesCount=column_profile.values_count or computed_profile.values_count,
                    )

        # Step 4: If no profile at all but have sample data, compute entirely from sample
        if column_name and column_profile is None and result.sample_data is not None:
            logger.info(f"No profile available for {column_name}, computing from sample data")
            column_profile = compute_column_stats_from_sample(result.sample_data, column_name)

        # Log if we still don't have a profile
        if column_name and column_profile is None:
            result.fetch_errors.append(
                f"No profile available for column {column_name}. "
                "Run Profiler ingestion with 'Compute Metrics' enabled."
            )

        if row_count is None and column_profile is None:
            result.fetch_errors.append(f"No profile available for table {table_fqn}")
            return None

        return column_profile, row_count

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
