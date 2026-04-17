"""Tests for FailureAnalyzer."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from dq_autofix.analyzer.failure_analyzer import AnalysisResult, FailureAnalyzer
from dq_autofix.analyzer.pattern_detector import DetectedPattern, PatternType
from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TableProfile,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies.base import ConfidenceResult, FailureContext


class TestAnalysisResult:
    """Tests for AnalysisResult dataclass."""

    @pytest.fixture
    def test_case(self) -> TestCaseResult:
        """Create a test case fixture."""
        return TestCaseResult(
            id="tc-001",
            name="test_nulls",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=50,
            ),
        )

    def test_has_recommendations_true(self, test_case: TestCaseResult) -> None:
        """Test has_recommendations returns True when recommendations exist."""
        context = FailureContext(test_case=test_case)
        mock_strategy = MagicMock()
        mock_confidence = ConfidenceResult(score=0.8)

        result = AnalysisResult(
            context=context,
            recommendations=[(mock_strategy, mock_confidence)],
        )
        assert result.has_recommendations is True

    def test_has_recommendations_false(self, test_case: TestCaseResult) -> None:
        """Test has_recommendations returns False when empty."""
        context = FailureContext(test_case=test_case)
        result = AnalysisResult(context=context)
        assert result.has_recommendations is False

    def test_top_confidence_with_best(self, test_case: TestCaseResult) -> None:
        """Test top_confidence returns best strategy score."""
        context = FailureContext(test_case=test_case)
        mock_strategy = MagicMock()
        mock_confidence = ConfidenceResult(score=0.85)

        result = AnalysisResult(
            context=context,
            best_strategy=(mock_strategy, mock_confidence),
        )
        assert result.top_confidence == 0.85

    def test_top_confidence_without_best(self, test_case: TestCaseResult) -> None:
        """Test top_confidence returns 0 when no best strategy."""
        context = FailureContext(test_case=test_case)
        result = AnalysisResult(context=context)
        assert result.top_confidence == 0.0

    def test_pattern_summary_with_patterns(self, test_case: TestCaseResult) -> None:
        """Test pattern_summary with patterns."""
        context = FailureContext(test_case=test_case)
        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50),
            DetectedPattern(PatternType.NUMERIC_DISTRIBUTION, 0.7, 0),
        ]
        result = AnalysisResult(context=context, patterns=patterns)

        summary = result.pattern_summary
        assert "sparse_nulls" in summary
        assert "90%" in summary

    def test_pattern_summary_no_patterns(self, test_case: TestCaseResult) -> None:
        """Test pattern_summary with no patterns."""
        context = FailureContext(test_case=test_case)
        result = AnalysisResult(context=context)
        assert result.pattern_summary == "No patterns detected"


class TestFailureAnalyzer:
    """Tests for FailureAnalyzer class."""

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
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=50,
                failed_rows_percentage=5.0,
            ),
        )

    @pytest.fixture
    def sample_data(self) -> SampleData:
        """Create sample data fixture."""
        return SampleData(
            table_fqn="db.schema.customers",
            columns=["id", "email"],
            rows=[
                [1, "test@example.com"],
                [2, None],
                [3, "valid@test.com"],
            ],
        )

    @pytest.fixture
    def table_profile(self) -> TableProfile:
        """Create table profile fixture."""
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
            ],
        )

    async def test_analyze_success(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test successful analysis of a failure."""
        mock_client.get_test_case_result.return_value = test_case
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = table_profile

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.analyze("tc-001")

        assert result.context.test_case == test_case
        assert "test_type" in result.analysis_metadata
        assert result.analysis_metadata["test_type"] == "columnValuesToNotBeNull"
        assert "analysis_duration_ms" in result.analysis_metadata

    async def test_analyze_not_found_by_name(
        self,
        mock_client: AsyncMock,
    ) -> None:
        """Test analysis fails when test case not found."""
        mock_client.get_test_case_result.return_value = None
        mock_client.get_test_case_by_id.return_value = None

        analyzer = FailureAnalyzer(mock_client)

        with pytest.raises(ValueError, match="Test case not found"):
            await analyzer.analyze("nonexistent")

    async def test_analyze_found_by_id(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test analysis finds test case by ID when name lookup fails."""
        mock_client.get_test_case_result.return_value = None
        mock_client.get_test_case_by_id.return_value = test_case
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = table_profile

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.analyze("tc-001")

        assert result.context.test_case == test_case
        mock_client.get_test_case_by_id.assert_called_once_with("tc-001")

    async def test_analyze_context(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
    ) -> None:
        """Test analysis from existing context."""
        profile = ColumnProfile(name="email", data_type="VARCHAR", null_proportion=0.05)
        context = FailureContext(test_case=test_case, column_profile=profile)

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.analyze_context(context)

        assert result.context == context
        assert len(result.patterns) > 0
        assert "pattern_clarity" in result.analysis_metadata

    async def test_analyze_context_with_recommendations(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
    ) -> None:
        """Test analysis produces recommendations."""
        profile = ColumnProfile(
            name="email",
            data_type="FLOAT",
            null_proportion=0.05,
            mean=100.0,
            median=100.0,
        )
        context = FailureContext(test_case=test_case, column_profile=profile)

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.analyze_context(context)

        assert result.has_recommendations is True
        assert result.best_strategy is not None
        assert result.best_strategy[1].score >= 0.4

    async def test_analyze_multiple(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test analyzing multiple failures."""
        mock_client.get_test_case_result.return_value = test_case
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = table_profile

        analyzer = FailureAnalyzer(mock_client)
        results = await analyzer.analyze_multiple(["tc-001", "tc-002"])

        assert len(results) == 2

    async def test_analyze_multiple_skips_not_found(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test analyze_multiple skips not found test cases."""
        mock_client.get_test_case_result.side_effect = [test_case, None]
        mock_client.get_test_case_by_id.return_value = None
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = table_profile

        analyzer = FailureAnalyzer(mock_client)
        results = await analyzer.analyze_multiple(["tc-001", "nonexistent"])

        assert len(results) == 1

    async def test_get_best_fix(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
        sample_data: SampleData,
        table_profile: TableProfile,
    ) -> None:
        """Test getting best fix for a test case."""
        mock_client.get_test_case_result.return_value = test_case
        mock_client.get_table_sample_data.return_value = sample_data
        mock_client.get_table_profile.return_value = table_profile

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.get_best_fix("tc-001")

        assert result is not None
        strategy, confidence, context = result
        assert strategy is not None
        assert confidence.score >= 0.4
        assert context.test_case == test_case

    async def test_get_best_fix_not_found(
        self,
        mock_client: AsyncMock,
    ) -> None:
        """Test get_best_fix returns None when test case not found."""
        mock_client.get_test_case_result.return_value = None
        mock_client.get_test_case_by_id.return_value = None

        analyzer = FailureAnalyzer(mock_client)
        result = await analyzer.get_best_fix("nonexistent")

        assert result is None

    def test_generate_fix_preview(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
    ) -> None:
        """Test generating fix preview from analysis result."""
        profile = ColumnProfile(
            name="email",
            data_type="FLOAT",
            null_proportion=0.05,
            mean=100.0,
            median=100.0,
        )
        context = FailureContext(test_case=test_case, column_profile=profile)

        mock_strategy = MagicMock()
        mock_strategy.name = "mean_imputation"
        mock_strategy.description = "Replace nulls with mean"
        mock_strategy.can_apply.return_value = True
        mock_strategy.preview.return_value = MagicMock(
            before_sample=[{"col": None}],
            after_sample=[{"col": 100.0}],
            changes_summary="Replace 50 nulls",
            affected_rows=50,
            total_rows=1000,
            affected_percentage=5.0,
        )
        mock_strategy.generate_fix_sql.return_value = "UPDATE table SET col = 100"
        mock_strategy.generate_rollback_sql.return_value = None

        mock_confidence = ConfidenceResult(score=0.8, breakdown={"data_coverage": 0.9})

        result = AnalysisResult(
            context=context,
            patterns=[DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50)],
            best_strategy=(mock_strategy, mock_confidence),
        )

        analyzer = FailureAnalyzer(mock_client)
        preview = analyzer.generate_fix_preview(result)

        assert preview["strategy"] == "mean_imputation"
        assert preview["confidence"] == 0.8
        assert "preview" in preview
        assert "fix_sql" in preview

    def test_generate_fix_preview_no_strategy(
        self,
        mock_client: AsyncMock,
        test_case: TestCaseResult,
    ) -> None:
        """Test generate_fix_preview with no strategy."""
        context = FailureContext(test_case=test_case)
        result = AnalysisResult(context=context)

        analyzer = FailureAnalyzer(mock_client)
        preview = analyzer.generate_fix_preview(result)

        assert "error" in preview
