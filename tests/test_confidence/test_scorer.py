"""Tests for ConfidenceScorer."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from dq_autofix.analyzer.pattern_detector import DetectedPattern, PatternType
from dq_autofix.confidence.scorer import ConfidenceScorer
from dq_autofix.openmetadata.models import (
    ColumnProfile,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies.base import ConfidenceResult, FailureContext


class TestConfidenceScorer:
    """Tests for ConfidenceScorer class."""

    @pytest.fixture
    def scorer(self) -> ConfidenceScorer:
        """Create a ConfidenceScorer instance."""
        return ConfidenceScorer()

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
                failed_rows_percentage=5.0,
            ),
        )

    @pytest.fixture
    def context(self, test_case: TestCaseResult) -> FailureContext:
        """Create a failure context fixture."""
        profile = ColumnProfile(
            name="col",
            data_type="FLOAT",
            null_proportion=0.05,
            mean=100.0,
            median=100.0,
        )
        return FailureContext(test_case=test_case, column_profile=profile)

    @pytest.fixture
    def mock_strategy(self) -> MagicMock:
        """Create a mock strategy."""
        strategy = MagicMock()
        strategy.name = "mean_imputation"
        strategy.description = "Replace nulls with mean"
        strategy.reversibility_score = 0.5
        strategy.can_apply.return_value = True
        strategy.calculate_confidence.return_value = ConfidenceResult(
            score=0.75,
            breakdown={
                "data_coverage": 0.8,
                "pattern_clarity": 0.7,
                "reversibility": 0.5,
                "impact_scope": 0.9,
                "type_match": 1.0,
            },
            reason="Numeric column with low null percentage",
        )
        return strategy

    def test_score_strategy_without_patterns(
        self,
        scorer: ConfidenceScorer,
        mock_strategy: MagicMock,
        context: FailureContext,
    ) -> None:
        """Test scoring strategy without patterns returns base confidence."""
        result = scorer.score_strategy(mock_strategy, context)

        assert result.score == 0.75
        assert result.breakdown == mock_strategy.calculate_confidence.return_value.breakdown

    def test_score_strategy_with_patterns(
        self,
        scorer: ConfidenceScorer,
        mock_strategy: MagicMock,
        context: FailureContext,
    ) -> None:
        """Test scoring strategy with patterns adjusts confidence."""
        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50),
        ]

        result = scorer.score_strategy(mock_strategy, context, patterns)

        assert "pattern_clarity" in result.breakdown
        assert result.breakdown["pattern_clarity"] == 0.9

    def test_score_strategy_not_applicable(
        self,
        scorer: ConfidenceScorer,
        mock_strategy: MagicMock,
        context: FailureContext,
    ) -> None:
        """Test scoring returns zero for non-applicable strategy."""
        mock_strategy.can_apply.return_value = False

        result = scorer.score_strategy(mock_strategy, context)

        assert result.score == 0.0
        assert "not applicable" in result.reason.lower()

    def test_score_strategy_with_matching_pattern_boost(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test scoring boosts confidence for matching patterns."""
        strategy = MagicMock()
        strategy.name = "trim_whitespace"
        strategy.can_apply.return_value = True
        strategy.calculate_confidence.return_value = ConfidenceResult(
            score=0.8,
            breakdown={
                "data_coverage": 0.8,
                "pattern_clarity": 0.8,
                "reversibility": 1.0,
                "impact_scope": 0.9,
                "type_match": 0.9,
            },
        )

        patterns = [
            DetectedPattern(PatternType.WHITESPACE_ISSUES, 0.95, 100),
        ]

        result = scorer.score_strategy(strategy, context, patterns)

        assert "pattern_boost" in result.breakdown
        assert result.breakdown["pattern_boost"] > 0

    def test_score_strategy_with_conflicting_pattern_penalty(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test scoring penalizes confidence for conflicting patterns."""
        strategy = MagicMock()
        strategy.name = "mean_imputation"
        strategy.can_apply.return_value = True
        strategy.calculate_confidence.return_value = ConfidenceResult(
            score=0.7,
            breakdown={
                "data_coverage": 0.8,
                "pattern_clarity": 0.6,
                "reversibility": 0.5,
                "impact_scope": 0.8,
                "type_match": 1.0,
            },
        )

        patterns = [
            DetectedPattern(PatternType.OUTLIERS, 0.9, 50),
        ]

        result = scorer.score_strategy(strategy, context, patterns)

        assert "pattern_boost" in result.breakdown
        assert result.breakdown["pattern_boost"] < 0

    def test_calculate_pattern_clarity_single(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test pattern clarity with single pattern."""
        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.85, 50),
        ]

        clarity = scorer._calculate_pattern_clarity(context, patterns)
        assert clarity == 0.85

    def test_calculate_pattern_clarity_multiple(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test pattern clarity with multiple patterns."""
        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50),
            DetectedPattern(PatternType.NUMERIC_DISTRIBUTION, 0.7, 0),
            DetectedPattern(PatternType.OUTLIERS, 0.5, 10),
        ]

        clarity = scorer._calculate_pattern_clarity(context, patterns)

        assert 0.5 <= clarity <= 1.0

    def test_calculate_pattern_clarity_empty(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test pattern clarity returns default for empty patterns."""
        clarity = scorer._calculate_pattern_clarity(context, [])
        assert clarity == 0.5

    def test_calculate_pattern_boost_affinity(
        self,
        scorer: ConfidenceScorer,
    ) -> None:
        """Test pattern boost for strategy-pattern affinity."""
        strategy = MagicMock()
        strategy.name = "mean_imputation"

        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50),
        ]

        boost = scorer._calculate_pattern_boost(strategy, patterns)
        assert boost > 0

    def test_calculate_pattern_boost_conflict(
        self,
        scorer: ConfidenceScorer,
    ) -> None:
        """Test pattern boost for strategy-pattern conflict."""
        strategy = MagicMock()
        strategy.name = "mean_imputation"

        patterns = [
            DetectedPattern(PatternType.OUTLIERS, 0.9, 50),
        ]

        boost = scorer._calculate_pattern_boost(strategy, patterns)
        assert boost < 0

    def test_calculate_pattern_boost_bounded(
        self,
        scorer: ConfidenceScorer,
    ) -> None:
        """Test pattern boost is bounded."""
        strategy = MagicMock()
        strategy.name = "trim_whitespace"

        patterns = [
            DetectedPattern(PatternType.WHITESPACE_ISSUES, 1.0, 1000),
            DetectedPattern(PatternType.WHITESPACE_ISSUES, 1.0, 1000),
            DetectedPattern(PatternType.WHITESPACE_ISSUES, 1.0, 1000),
        ]

        boost = scorer._calculate_pattern_boost(strategy, patterns)
        assert -0.15 <= boost <= 0.15

    def test_score_multiple_strategies(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test scoring multiple strategies."""
        strategy1 = MagicMock()
        strategy1.name = "mean_imputation"
        strategy1.can_apply.return_value = True
        strategy1.calculate_confidence.return_value = ConfidenceResult(
            score=0.8,
            breakdown={
                "data_coverage": 0.8,
                "pattern_clarity": 0.8,
                "reversibility": 0.5,
                "impact_scope": 0.9,
                "type_match": 1.0,
            },
        )

        strategy2 = MagicMock()
        strategy2.name = "median_imputation"
        strategy2.can_apply.return_value = True
        strategy2.calculate_confidence.return_value = ConfidenceResult(
            score=0.6,
            breakdown={
                "data_coverage": 0.6,
                "pattern_clarity": 0.6,
                "reversibility": 0.5,
                "impact_scope": 0.7,
                "type_match": 0.8,
            },
        )

        strategy3 = MagicMock()
        strategy3.name = "mode_imputation"
        strategy3.can_apply.return_value = True
        strategy3.calculate_confidence.return_value = ConfidenceResult(
            score=0.3,
            breakdown={
                "data_coverage": 0.3,
                "pattern_clarity": 0.3,
                "reversibility": 0.3,
                "impact_scope": 0.3,
                "type_match": 0.3,
            },
        )

        results = scorer.score_multiple_strategies(
            [strategy1, strategy2, strategy3],
            context,
            min_confidence=0.4,
        )

        assert len(results) == 2
        assert results[0][0].name == "mean_imputation"
        assert results[1][0].name == "median_imputation"

    def test_score_multiple_strategies_with_patterns(
        self,
        scorer: ConfidenceScorer,
        context: FailureContext,
    ) -> None:
        """Test scoring multiple strategies with patterns."""
        strategy = MagicMock()
        strategy.name = "mean_imputation"
        strategy.can_apply.return_value = True
        strategy.calculate_confidence.return_value = ConfidenceResult(
            score=0.7,
            breakdown={
                "data_coverage": 0.7,
                "pattern_clarity": 0.7,
                "reversibility": 0.5,
                "impact_scope": 0.8,
                "type_match": 0.9,
            },
        )

        patterns = [DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50)]

        results = scorer.score_multiple_strategies([strategy], context, patterns)

        assert len(results) == 1
        assert "pattern_clarity" in results[0][1].breakdown

    def test_explain_confidence(
        self,
        scorer: ConfidenceScorer,
        mock_strategy: MagicMock,
    ) -> None:
        """Test generating human-readable explanation."""
        confidence = ConfidenceResult(
            score=0.75,
            breakdown={
                "data_coverage": 0.8,
                "pattern_clarity": 0.7,
                "reversibility": 0.5,
                "impact_scope": 0.9,
                "type_match": 1.0,
            },
            reason="Numeric column with low null percentage",
        )

        explanation = scorer.explain_confidence(confidence, mock_strategy)

        assert "75.0%" in explanation
        assert "mean_imputation" in explanation
        assert "Data Coverage" in explanation
        assert "Numeric column" in explanation

    def test_explain_confidence_with_patterns(
        self,
        scorer: ConfidenceScorer,
        mock_strategy: MagicMock,
    ) -> None:
        """Test explanation includes patterns."""
        confidence = ConfidenceResult(score=0.8, breakdown={})
        patterns = [
            DetectedPattern(PatternType.SPARSE_NULLS, 0.9, 50),
            DetectedPattern(PatternType.NUMERIC_DISTRIBUTION, 0.7, 0),
        ]

        explanation = scorer.explain_confidence(confidence, mock_strategy, patterns)

        assert "Detected Patterns" in explanation
        assert "sparse_nulls" in explanation

    def test_custom_weights(self, context: FailureContext) -> None:
        """Test scorer with custom weights."""
        scorer = ConfidenceScorer(
            coverage_weight=0.3,
            pattern_clarity_weight=0.3,
            reversibility_weight=0.1,
            impact_scope_weight=0.15,
            type_match_weight=0.15,
        )

        assert scorer.coverage_weight == 0.3
        assert scorer.pattern_clarity_weight == 0.3
        assert scorer.reversibility_weight == 0.1
