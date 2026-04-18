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

    test_case_id: str | None = Field(default=None, alias="testCaseId")
    table_fqn: str | None = Field(default=None, alias="tableFqn")

    model_config = {"populate_by_name": True}


class SuggestRequest(BaseModel):
    """Request to suggest a fix for a failure."""

    failure_id: str = Field(alias="failureId")
    strategy_override: str | None = Field(default=None, alias="strategyOverride")

    model_config = {"populate_by_name": True}


class PreviewRequest(BaseModel):
    """Request to preview a fix."""

    failure_id: str = Field(alias="failureId")
    strategy: str

    model_config = {"populate_by_name": True}


class PatternResponse(BaseModel):
    """Detected pattern in API response."""

    pattern_type: str = Field(serialization_alias="patternType")
    confidence: float
    affected_count: int = Field(serialization_alias="affectedCount")
    details: dict[str, Any] = Field(default_factory=dict)


class ConfidenceBreakdownResponse(BaseModel):
    """Confidence score breakdown in API response."""

    data_coverage: float = Field(serialization_alias="dataCoverage")
    pattern_clarity: float = Field(serialization_alias="patternClarity")
    reversibility: float
    impact_scope: float = Field(serialization_alias="impactScope")
    type_match: float = Field(serialization_alias="typeMatch")
    pattern_boost: float | None = Field(default=None, serialization_alias="patternBoost")


class StrategyRecommendationResponse(BaseModel):
    """Strategy recommendation in API response."""

    name: str
    description: str
    confidence_score: float = Field(serialization_alias="confidenceScore")
    confidence_breakdown: dict[str, float] = Field(serialization_alias="confidenceBreakdown")
    reason: str = ""


class PreviewDataResponse(BaseModel):
    """Preview data showing before/after samples."""

    before_sample: list[dict[str, Any]] = Field(serialization_alias="beforeSample")
    after_sample: list[dict[str, Any]] = Field(serialization_alias="afterSample")
    changes_summary: str = Field(serialization_alias="changesSummary")
    affected_rows: int = Field(serialization_alias="affectedRows")
    total_rows: int | None = Field(default=None, serialization_alias="totalRows")
    affected_percentage: float | None = Field(
        default=None, serialization_alias="affectedPercentage"
    )


class AnalysisMetadataResponse(BaseModel):
    """Analysis metadata in API response."""

    test_type: str = Field(serialization_alias="testType")
    table_fqn: str = Field(serialization_alias="tableFqn")
    column_name: str | None = Field(default=None, serialization_alias="columnName")
    failed_rows: int | None = Field(default=None, serialization_alias="failedRows")
    failed_percentage: float | None = Field(default=None, serialization_alias="failedPercentage")
    pattern_clarity: float = Field(serialization_alias="patternClarity")
    strategies_evaluated: int = Field(serialization_alias="strategiesEvaluated")
    strategies_recommended: int = Field(serialization_alias="strategiesRecommended")
    analysis_duration_ms: float = Field(serialization_alias="analysisDurationMs")
    fetch_errors: list[str] = Field(default_factory=list, serialization_alias="fetchErrors")


class AnalyzeResponse(BaseModel):
    """Response from analyze endpoint."""

    test_case_id: str = Field(serialization_alias="testCaseId")
    test_case_name: str = Field(serialization_alias="testCaseName")
    patterns: list[PatternResponse] = Field(default_factory=list)
    recommendations: list[StrategyRecommendationResponse] = Field(default_factory=list)
    best_strategy: StrategyRecommendationResponse | None = Field(
        default=None, serialization_alias="bestStrategy"
    )
    metadata: AnalysisMetadataResponse


class SuggestResponse(BaseModel):
    """Response from suggest endpoint."""

    failure_id: str = Field(serialization_alias="failureId")
    strategy: str
    strategy_description: str = Field(serialization_alias="strategyDescription")
    confidence_score: float = Field(serialization_alias="confidenceScore")
    confidence_breakdown: dict[str, float] = Field(serialization_alias="confidenceBreakdown")
    preview: PreviewDataResponse
    fix_sql: str = Field(serialization_alias="fixSql")
    rollback_sql: str | None = Field(default=None, serialization_alias="rollbackSql")
    patterns: list[PatternResponse] = Field(default_factory=list)
