"""Pydantic schemas for API request/response models."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dq_autofix.openmetadata.models import TestResultStatus


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "healthy"
    version: str
    openmetadata_host: str


class TestResultValueResponse(BaseModel):
    """Individual test result value in API response."""

    name: str
    value: str | None = None


class TestResultResponse(BaseModel):
    """Test result summary in API response."""

    status: TestResultStatus
    timestamp: datetime
    failed_rows: int | None = Field(default=None, serialization_alias="failedRows")
    passed_rows: int | None = Field(default=None, serialization_alias="passedRows")
    failed_rows_percentage: float | None = Field(
        default=None, serialization_alias="failedRowsPercentage"
    )
    passed_rows_percentage: float | None = Field(
        default=None, serialization_alias="passedRowsPercentage"
    )
    test_result_values: list[TestResultValueResponse] | None = Field(
        default=None, serialization_alias="testResultValues"
    )


class FailureResponse(BaseModel):
    """Single DQ failure in API response."""

    id: str
    name: str
    display_name: str | None = Field(default=None, serialization_alias="displayName")
    description: str | None = None
    test_definition: str = Field(serialization_alias="testDefinition")
    table_fqn: str = Field(serialization_alias="tableFqn")
    column_name: str | None = Field(default=None, serialization_alias="columnName")
    test_suite: str | None = Field(default=None, serialization_alias="testSuite")
    parameter_values: list[dict[str, Any]] | None = Field(
        default=None, serialization_alias="parameterValues"
    )
    result: TestResultResponse | None = None


class FailureListResponse(BaseModel):
    """List of DQ failures in API response."""

    data: list[FailureResponse]
    total: int


class ErrorResponse(BaseModel):
    """Error response."""

    detail: str
    error_code: str | None = Field(default=None, serialization_alias="errorCode")


class AnalyzeRequest(BaseModel):
    """Request to analyze a test case or table."""

    test_case_id: str | None = Field(default=None, serialization_alias="testCaseId")
    table_fqn: str | None = Field(default=None, serialization_alias="tableFqn")


class SuggestRequest(BaseModel):
    """Request to suggest a fix for a failure."""

    failure_id: str = Field(serialization_alias="failureId")
    strategy_override: str | None = Field(default=None, serialization_alias="strategyOverride")


class PreviewRequest(BaseModel):
    """Request to preview a fix."""

    failure_id: str = Field(serialization_alias="failureId")
    strategy: str
