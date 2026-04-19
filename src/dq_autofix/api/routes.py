"""FastAPI route handlers."""

from fastapi import APIRouter, Depends, HTTPException, Request, status

from dq_autofix import __version__
from dq_autofix.analyzer import FailureAnalyzer
from dq_autofix.api.schemas import (
    AnalysisMetadataResponse,
    AnalyzeRequest,
    AnalyzeResponse,
    ErrorResponse,
    FailureListResponse,
    FailureResponse,
    HealthResponse,
    PatternResponse,
    PreviewDataResponse,
    PreviewRequest,
    StrategyRecommendationResponse,
    SuggestRequest,
    SuggestResponse,
    TestResultResponse,
    TestResultValueResponse,
)
from dq_autofix.config import Settings, get_settings
from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.openmetadata.models import TestCaseResult
from dq_autofix.strategies.registry import get_default_registry

router = APIRouter()


def get_om_client(request: Request) -> OpenMetadataClient:
    """Get OpenMetadata client from app state."""
    client: OpenMetadataClient = request.app.state.om_client
    return client


def _convert_to_failure_response(tc: TestCaseResult) -> FailureResponse:
    """Convert TestCaseResult to FailureResponse."""
    result_response = None
    if tc.result:
        test_values = None
        if tc.result.test_result_value:
            test_values = [
                TestResultValueResponse(name=v.name, value=v.value)
                for v in tc.result.test_result_value
            ]
        result_response = TestResultResponse(
            status=tc.result.status,
            timestamp=tc.result.timestamp,
            failed_rows=tc.result.failed_rows,
            passed_rows=tc.result.passed_rows,
            failed_rows_percentage=tc.result.failed_rows_percentage,
            passed_rows_percentage=tc.result.passed_rows_percentage,
            test_result_values=test_values,
        )

    return FailureResponse(
        id=tc.id,
        name=tc.name,
        display_name=tc.display_name,
        description=tc.description,
        test_definition=tc.test_definition or "unknown",
        table_fqn=tc.table_fqn,
        column_name=tc.column_name,
        test_suite=tc.test_suite,
        parameter_values=tc.parameter_values,
        result=result_response,
    )


@router.get(
    "/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health check",
)
async def health_check(settings: Settings = Depends(get_settings)) -> HealthResponse:
    """Check service health status."""
    return HealthResponse(
        status="healthy",
        version=__version__,
        openmetadata_host=settings.openmetadata_host,
    )


@router.get(
    "/databases",
    tags=["Filters"],
    summary="List available databases from failed test cases",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def list_databases(
    client: OpenMetadataClient = Depends(get_om_client),
) -> dict[str, object]:
    """Get list of unique database paths (service.database.schema) that have failed test cases."""
    try:
        failures = await client.get_failed_test_cases(database_filter=None)
        databases: set[str] = set()
        for f in failures:
            if f.table_fqn:
                parts = f.table_fqn.split(".")
                if len(parts) >= 3:
                    # Format: service.database.schema.table -> extract service.database.schema
                    db_path = ".".join(parts[:3])
                    databases.add(db_path)
        return {
            "data": sorted(databases),
            "total": len(databases),
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch databases: {e}",
        ) from e


@router.get(
    "/failures",
    response_model=FailureListResponse,
    tags=["Failures"],
    summary="List failed DQ tests",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def list_failures(
    database: str | None = None,
    client: OpenMetadataClient = Depends(get_om_client),
) -> FailureListResponse:
    """List all failed data quality test cases.

    Args:
        database: Optional database path to filter by (e.g., 'test_mysql.default.test_dq').
    """
    try:
        failures = await client.get_failed_test_cases(database_filter=database)
        return FailureListResponse(
            data=[_convert_to_failure_response(f) for f in failures],
            total=len(failures),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch failures: {e}",
        ) from e


@router.get(
    "/failures/{failure_id}",
    response_model=FailureResponse,
    tags=["Failures"],
    summary="Get failure details",
    responses={
        404: {"model": ErrorResponse, "description": "Failure not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_failure(
    failure_id: str,
    client: OpenMetadataClient = Depends(get_om_client),
) -> FailureResponse:
    """Get details of a specific failed test case."""
    try:
        failure = await client.get_test_case_result(failure_id)
        if failure is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Failure with id '{failure_id}' not found",
            )
        return _convert_to_failure_response(failure)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch failure: {e}",
        ) from e


@router.post(
    "/analyze",
    response_model=AnalyzeResponse,
    tags=["Analysis"],
    summary="Analyze a DQ failure",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        404: {"model": ErrorResponse, "description": "Test case not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def analyze_failure(
    request: AnalyzeRequest,
    client: OpenMetadataClient = Depends(get_om_client),
) -> AnalyzeResponse:
    """Analyze a DQ failure and get fix recommendations.

    Performs pattern detection and strategy recommendation for the
    specified test case. Returns detected patterns, recommended strategies
    with confidence scores, and the best strategy recommendation.
    """
    if not request.test_case_id and not request.table_fqn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either testCaseId or tableFqn must be provided",
        )

    test_case_id = request.test_case_id or request.table_fqn
    if test_case_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Test case ID is required",
        )

    try:
        analyzer = FailureAnalyzer(client)
        result = await analyzer.analyze(test_case_id)

        patterns = [
            PatternResponse(
                pattern_type=p.pattern_type.value,
                confidence=p.confidence,
                affected_count=p.affected_count,
                details=p.details,
            )
            for p in result.patterns
        ]

        recommendations = [
            StrategyRecommendationResponse(
                name=strategy.name,
                description=strategy.description,
                confidence_score=confidence.score,
                confidence_breakdown=confidence.breakdown,
                reason=confidence.reason,
            )
            for strategy, confidence in result.recommendations
        ]

        best_strategy = None
        if result.best_strategy:
            strategy, confidence = result.best_strategy
            best_strategy = StrategyRecommendationResponse(
                name=strategy.name,
                description=strategy.description,
                confidence_score=confidence.score,
                confidence_breakdown=confidence.breakdown,
                reason=confidence.reason,
            )

        metadata = AnalysisMetadataResponse(
            test_type=result.analysis_metadata.get("test_type", ""),
            table_fqn=result.analysis_metadata.get("table_fqn", ""),
            column_name=result.analysis_metadata.get("column_name"),
            failed_rows=result.analysis_metadata.get("failed_rows"),
            failed_percentage=result.analysis_metadata.get("failed_percentage"),
            pattern_clarity=result.analysis_metadata.get("pattern_clarity", 0.0),
            strategies_evaluated=result.analysis_metadata.get("strategies_evaluated", 0),
            strategies_recommended=result.analysis_metadata.get("strategies_recommended", 0),
            analysis_duration_ms=result.analysis_metadata.get("analysis_duration_ms", 0.0),
            fetch_errors=result.analysis_metadata.get("fetch_errors", []),
        )

        return AnalyzeResponse(
            test_case_id=result.context.test_case.id,
            test_case_name=result.context.test_case.name,
            patterns=patterns,
            recommendations=recommendations,
            best_strategy=best_strategy,
            metadata=metadata,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {e}",
        ) from e


@router.post(
    "/suggest",
    response_model=SuggestResponse,
    tags=["Analysis"],
    summary="Get fix suggestion with preview",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        404: {"model": ErrorResponse, "description": "Test case or strategy not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def suggest_fix(
    request: SuggestRequest,
    client: OpenMetadataClient = Depends(get_om_client),
) -> SuggestResponse:
    """Get a fix suggestion with preview for a DQ failure.

    Analyzes the failure, selects the best strategy (or uses the override),
    and returns a complete fix suggestion including SQL and preview.
    """
    try:
        analyzer = FailureAnalyzer(client)
        result = await analyzer.analyze(request.failure_id)

        strategy = None
        confidence = None

        if request.strategy_override:
            registry = get_default_registry()
            strategy = registry.get_strategy_by_name(request.strategy_override)
            if strategy is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Strategy '{request.strategy_override}' not found",
                )
            if not strategy.can_apply(result.context):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Strategy '{request.strategy_override}' cannot be applied",
                )
            confidence = strategy.calculate_confidence(result.context)
        elif result.best_strategy:
            strategy, confidence = result.best_strategy
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No applicable fix strategy found",
            )

        preview = strategy.preview(result.context)
        fix_sql = strategy.generate_fix_sql(result.context)
        rollback_sql = strategy.generate_rollback_sql(result.context)

        patterns = [
            PatternResponse(
                pattern_type=p.pattern_type.value,
                confidence=p.confidence,
                affected_count=p.affected_count,
                details=p.details,
            )
            for p in result.patterns
        ]

        return SuggestResponse(
            failure_id=request.failure_id,
            strategy=strategy.name,
            strategy_description=strategy.description,
            confidence_score=confidence.score,
            confidence_breakdown=confidence.breakdown,
            preview=PreviewDataResponse(
                before_sample=preview.before_sample,
                after_sample=preview.after_sample,
                changes_summary=preview.changes_summary,
                affected_rows=preview.affected_rows,
                total_rows=preview.total_rows,
                affected_percentage=preview.affected_percentage,
            ),
            fix_sql=fix_sql,
            rollback_sql=rollback_sql,
            patterns=patterns,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Suggestion failed: {e}",
        ) from e


@router.post(
    "/preview",
    response_model=SuggestResponse,
    tags=["Analysis"],
    summary="Preview a fix for a specific strategy",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        404: {"model": ErrorResponse, "description": "Test case or strategy not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def preview_fix(
    request: PreviewRequest,
    client: OpenMetadataClient = Depends(get_om_client),
) -> SuggestResponse:
    """Preview a fix without full analysis.

    Similar to /suggest but requires explicit strategy selection.
    Useful when you want to preview a specific strategy without
    running the full recommendation engine.
    """
    suggest_request = SuggestRequest(
        failureId=request.failure_id,
        strategyOverride=request.strategy,
    )
    return await suggest_fix(suggest_request, client)


@router.get(
    "/strategies",
    tags=["Strategies"],
    summary="List available fix strategies",
)
async def list_strategies() -> dict[str, object]:
    """List all available fix strategies."""
    registry = get_default_registry()
    strategies = registry.get_all_strategies()

    return {
        "data": [
            {
                "name": s.name,
                "description": s.description,
                "supportedTestTypes": s.supported_test_types,
                "reversibilityScore": s.reversibility_score,
            }
            for s in strategies
        ],
        "total": len(strategies),
    }
