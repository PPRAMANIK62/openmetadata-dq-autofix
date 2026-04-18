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
                    # Extract test definition from name if not present
                    if "testDefinition" not in item and "name" in item:
                        name = item["name"]
                        # Extract test type from name pattern like "column_values_to_be_not_null"
                        if "column_values_to_be_not_null" in name.lower():
                            item["testDefinition"] = "columnValuesToNotBeNull"
                        elif "column_values_to_be_unique" in name.lower():
                            item["testDefinition"] = "columnValuesToBeUnique"
                        elif "column_values_to_match_regex" in name.lower():
                            item["testDefinition"] = "columnValuesToMatchRegex"
                        elif "column_values_to_be_in_set" in name.lower():
                            item["testDefinition"] = "columnValuesToBeInSet"
                        else:
                            item["testDefinition"] = "unknown"
                    results.append(TestCaseResult.model_validate(item))
                except Exception as e:
                    logger.warning(f"Failed to parse test case: {e}")

            return results

        except httpx.HTTPError as e:
            raise OpenMetadataClientError(f"Failed to fetch test cases: {e}") from e

    async def get_test_case_result(self, test_case_id: str) -> TestCaseResult | None:
        """Get a specific test case result by ID, name, or FQN.

        Args:
            test_case_id: The test case identifier (UUID, name, or FQN).

        Returns:
            Test case result or None if not found.
        """
        client = await self._get_http_client()

        # First try to find by searching all test cases (handles name matching)
        try:
            response = await client.get(
                "/api/v1/dataQuality/testCases",
                params={"limit": 200},
            )
            response.raise_for_status()
            data = response.json()

            for item in data.get("data", []):
                # Match by ID, name, or FQN
                if (
                    item.get("id") == test_case_id
                    or item.get("name") == test_case_id
                    or item.get("fullyQualifiedName") == test_case_id
                    or item.get("name", "").lower() == test_case_id.lower()
                ):
                    # Extract test definition from name if not present
                    if "testDefinition" not in item and "name" in item:
                        name = item["name"]
                        if "column_values_to_be_not_null" in name.lower():
                            item["testDefinition"] = "columnValuesToNotBeNull"
                        elif "column_values_to_be_unique" in name.lower():
                            item["testDefinition"] = "columnValuesToBeUnique"
                        elif "column_values_to_match_regex" in name.lower():
                            item["testDefinition"] = "columnValuesToMatchRegex"
                        elif "column_values_to_be_in_set" in name.lower():
                            item["testDefinition"] = "columnValuesToBeInSet"
                        else:
                            item["testDefinition"] = "unknown"
                    return TestCaseResult.model_validate(item)

            return None

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
            # OpenMetadata returns 500 for not found test cases (bug), handle gracefully
            if response.status_code in (404, 500):
                return None
            response.raise_for_status()
            return TestCaseResult.model_validate(response.json())

        except httpx.HTTPError as e:
            # Treat HTTP errors as not found for this lookup
            logger.warning(f"HTTP error looking up test case {test_case_id}: {e}")
            return None

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
            # First get the table ID
            encoded_fqn = table_fqn.replace("/", "%2F")
            table_response = await client.get(f"/api/v1/tables/name/{encoded_fqn}")
            if table_response.status_code == 404:
                return None
            table_response.raise_for_status()
            table_data = table_response.json()
            table_id = table_data.get("id")

            if not table_id:
                return None

            # Use table ID to get sample data
            response = await client.get(f"/api/v1/tables/{table_id}/sampleData")
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()

            # Extract sampleData from response if nested
            if "sampleData" in data:
                sample = data["sampleData"]
                sample["tableFqn"] = table_fqn
                return SampleData.model_validate(sample)
            elif "columns" in data and "rows" in data:
                data["tableFqn"] = table_fqn
                return SampleData.model_validate(data)

            return None

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
