"""Data models matching OpenMetadata API structures."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class TestResultStatus(StrEnum):
    """Status of a test case result."""

    SUCCESS = "Success"
    FAILED = "Failed"
    ABORTED = "Aborted"
    QUEUED = "Queued"


class TestDefinition(BaseModel):
    """Test definition type from OpenMetadata."""

    id: str
    name: str
    display_name: str | None = Field(default=None, alias="displayName")
    description: str | None = None
    test_platform: str = Field(default="OpenMetadata", alias="testPlatform")

    model_config = {"populate_by_name": True}


class TestResultValue(BaseModel):
    """Individual test result value."""

    name: str
    value: str | None = None


class TestCaseResultSummary(BaseModel):
    """Summary of test case execution result."""

    status: TestResultStatus
    timestamp: datetime
    failed_rows: int | None = Field(default=None, alias="failedRows")
    passed_rows: int | None = Field(default=None, alias="passedRows")
    failed_rows_percentage: float | None = Field(default=None, alias="failedRowsPercentage")
    passed_rows_percentage: float | None = Field(default=None, alias="passedRowsPercentage")
    test_result_value: list[TestResultValue] | None = Field(default=None, alias="testResultValue")

    model_config = {"populate_by_name": True}


class TestCaseResult(BaseModel):
    """Complete test case with result from OpenMetadata."""

    id: str
    name: str
    display_name: str | None = Field(default=None, alias="displayName")
    description: str | None = None
    test_definition: str = Field(alias="testDefinition")
    entity_link: str = Field(alias="entityLink")
    test_suite: str | None = Field(default=None, alias="testSuite")
    parameter_values: list[dict[str, Any]] | None = Field(default=None, alias="parameterValues")
    result: TestCaseResultSummary | None = None

    model_config = {"populate_by_name": True}

    @property
    def table_fqn(self) -> str:
        """Extract table fully qualified name from entity link."""
        link = self.entity_link
        if link.startswith("<#E::table::"):
            link = link[12:]
        if link.endswith(">"):
            link = link[:-1]
        if "::columns::" in link:
            link = link.split("::columns::")[0]
        return link

    @property
    def column_name(self) -> str | None:
        """Extract column name from entity link if present."""
        link = self.entity_link
        if "::columns::" in link:
            col = link.split("::columns::")[1]
            if col.endswith(">"):
                col = col[:-1]
            return col
        return None


class ColumnProfile(BaseModel):
    """Column statistics from table profile."""

    name: str
    data_type: str | None = Field(default=None, alias="dataType")
    null_count: int | None = Field(default=None, alias="nullCount")
    null_proportion: float | None = Field(default=None, alias="nullProportion")
    unique_count: int | None = Field(default=None, alias="uniqueCount")
    unique_proportion: float | None = Field(default=None, alias="uniqueProportion")
    distinct_count: int | None = Field(default=None, alias="distinctCount")
    min: float | str | None = None
    max: float | str | None = None
    mean: float | None = None
    median: float | None = None
    std_dev: float | None = Field(default=None, alias="stddev")
    sum: float | None = None
    values_count: int | None = Field(default=None, alias="valuesCount")
    missing_count: int | None = Field(default=None, alias="missingCount")
    missing_percentage: float | None = Field(default=None, alias="missingPercentage")
    duplicate_count: int | None = Field(default=None, alias="duplicateCount")
    histogram: dict[str, Any] | None = None
    custom_metrics: list[dict[str, Any]] | None = Field(default=None, alias="customMetrics")

    model_config = {"populate_by_name": True}


class TableProfile(BaseModel):
    """Table profile with column statistics."""

    table_fqn: str = Field(alias="tableFqn")
    timestamp: datetime
    row_count: int | None = Field(default=None, alias="rowCount")
    column_count: int | None = Field(default=None, alias="columnCount")
    size_in_bytes: int | None = Field(default=None, alias="sizeInBytes")
    columns: list[ColumnProfile] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    def get_column(self, name: str) -> ColumnProfile | None:
        """Get column profile by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


class SampleData(BaseModel):
    """Sample data rows from a table."""

    table_fqn: str = Field(alias="tableFqn")
    columns: list[str]
    rows: list[list[Any]]

    model_config = {"populate_by_name": True}

    def to_dicts(self) -> list[dict[str, Any]]:
        """Convert sample data to list of dictionaries."""
        return [dict(zip(self.columns, row, strict=False)) for row in self.rows]
