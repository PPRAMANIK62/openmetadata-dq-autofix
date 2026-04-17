"""OpenMetadata client wrapper."""

import logging
from typing import Any

import httpx

from dq_autofix.config import Settings
from dq_autofix.openmetadata.models import (
    SampleData,
    TableProfile,
    TestCaseResult,
)

logger = logging.getLogger(__name__)


class OpenMetadataClientError(Exception):
    """Error communicating with OpenMetadata API."""


class OpenMetadataClient:
    """Client for OpenMetadata API.

    Provides methods to fetch DQ test cases, sample data, and table profiles
    from a running OpenMetadata instance.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._http_client: httpx.AsyncClient | None = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for API calls."""
        if self._http_client is None:
            headers = {"Content-Type": "application/json"}
            if self.settings.openmetadata_token:
                headers["Authorization"] = f"Bearer {self.settings.openmetadata_token}"

            self._http_client = httpx.AsyncClient(
                base_url=self.settings.openmetadata_host,
                headers=headers,
                timeout=30.0,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client connection."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_failed_test_cases(self) -> list[TestCaseResult]:
        """Get all failed test cases.

        Returns:
            List of failed test case results.
        """
        client = await self._get_http_client()

        try:
            response = await client.get(
                "/api/v1/dataQuality/testCases",
                params={"testCaseStatus": "Failed", "limit": 100},
            )
            response.raise_for_status()
            data = response.json()

            results = []
            for item in data.get("data", []):
                try:
                    results.append(TestCaseResult.model_validate(item))
                except Exception as e:
                    logger.warning(f"Failed to parse test case: {e}")

            return results

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(f"Failed to fetch test cases: {e}") from e

    async def get_test_case_result(self, test_case_id: str) -> TestCaseResult | None:
        """Get a specific test case result by ID.

        Args:
            test_case_id: The test case identifier (can be ID or FQN).

        Returns:
            Test case result or None if not found.
        """
        client = await self._get_http_client()

        try:
            response = await client.get(
                f"/api/v1/dataQuality/testCases/name/{test_case_id}",
                params={"fields": "testDefinition,testSuite"},
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return TestCaseResult.model_validate(response.json())

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(f"Failed to fetch test case {test_case_id}: {e}") from e

    async def get_test_case_by_id(self, test_case_id: str) -> TestCaseResult | None:
        """Get a specific test case by UUID.

        Args:
            test_case_id: The test case UUID.

        Returns:
            Test case result or None if not found.
        """
        client = await self._get_http_client()

        try:
            response = await client.get(
                f"/api/v1/dataQuality/testCases/{test_case_id}",
                params={"fields": "testDefinition,testSuite"},
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return TestCaseResult.model_validate(response.json())

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(f"Failed to fetch test case {test_case_id}: {e}") from e

    async def get_table_sample_data(self, table_fqn: str, limit: int = 100) -> SampleData | None:
        """Get sample data rows from a table.

        Args:
            table_fqn: Fully qualified table name.
            limit: Maximum rows to return.

        Returns:
            Sample data or None if not available.
        """
        client = await self._get_http_client()

        try:
            encoded_fqn = table_fqn.replace("/", "%2F")
            response = await client.get(
                f"/api/v1/tables/name/{encoded_fqn}/sampleData",
                params={"limit": limit},
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            data["tableFqn"] = table_fqn
            return SampleData.model_validate(data)

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(
                f"Failed to fetch sample data for {table_fqn}: {e}"
            ) from e

    async def get_table_profile(self, table_fqn: str) -> TableProfile | None:
        """Get table profile with column statistics.

        Args:
            table_fqn: Fully qualified table name.

        Returns:
            Table profile or None if not available.
        """
        client = await self._get_http_client()

        try:
            encoded_fqn = table_fqn.replace("/", "%2F")
            response = await client.get(f"/api/v1/tables/name/{encoded_fqn}/tableProfile/latest")
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            data["tableFqn"] = table_fqn
            return TableProfile.model_validate(data)

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(
                f"Failed to fetch table profile for {table_fqn}: {e}"
            ) from e

    async def get_test_case_results(
        self, test_case_fqn: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get historical results for a test case.

        Args:
            test_case_fqn: Fully qualified test case name.
            limit: Maximum results to return.

        Returns:
            List of test case result records.
        """
        client = await self._get_http_client()

        try:
            response = await client.get(
                f"/api/v1/dataQuality/testCases/name/{test_case_fqn}/testCaseResult",
                params={"limit": limit},
            )
            if response.status_code == 404:
                return []
            response.raise_for_status()
            data: list[dict[str, Any]] = response.json().get("data", [])
            return data

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(
                f"Failed to fetch test case results for {test_case_fqn}: {e}"
            ) from e
