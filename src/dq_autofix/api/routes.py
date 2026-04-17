"""FastAPI route handlers."""

from fastapi import APIRouter, Depends, HTTPException, Request, status

from dq_autofix import __version__
from dq_autofix.api.schemas import (
    ErrorResponse,
    FailureListResponse,
    FailureResponse,
    HealthResponse,
    TestResultResponse,
    TestResultValueResponse,
)
from dq_autofix.config import Settings, get_settings
from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.openmetadata.models import TestCaseResult

router = APIRouter()


def get_om_client(request: Request) -> OpenMetadataClient:
    """Get OpenMetadata client from app state."""
    return request.app.state.om_client


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
        test_definition=tc.test_definition,
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
    "/failures",
    response_model=FailureListResponse,
    tags=["Failures"],
    summary="List failed DQ tests",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def list_failures(
    client: OpenMetadataClient = Depends(get_om_client),
) -> FailureListResponse:
    """List all failed data quality test cases."""
    try:
        failures = await client.get_failed_test_cases()
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
