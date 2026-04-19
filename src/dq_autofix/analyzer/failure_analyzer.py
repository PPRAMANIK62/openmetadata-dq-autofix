"""Failure analyzer orchestrator for DQ analysis."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from dq_autofix.analyzer.pattern_detector import DetectedPattern, PatternDetector
from dq_autofix.analyzer.sample_fetcher import SampleFetcher
from dq_autofix.confidence.scorer import ConfidenceScorer
from dq_autofix.openmetadata.client import OpenMetadataClient
from dq_autofix.strategies.base import ConfidenceResult, FailureContext, FixStrategy
from dq_autofix.strategies.registry import StrategyRegistry, get_default_registry


@dataclass
class AnalysisResult:
    """Complete result of analyzing a DQ failure.

    Contains the failure context, detected patterns, strategy recommendations,
    and metadata about the analysis process.
    """

    context: FailureContext
    patterns: list[DetectedPattern] = field(default_factory=list)
    recommendations: list[tuple[FixStrategy, ConfidenceResult]] = field(default_factory=list)
    best_strategy: tuple[FixStrategy, ConfidenceResult] | None = None
    analysis_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_recommendations(self) -> bool:
        """Check if any strategies were recommended."""
        return len(self.recommendations) > 0

    @property
    def top_confidence(self) -> float:
        """Get the highest confidence score among recommendations."""
        if self.best_strategy:
            return self.best_strategy[1].score
        return 0.0

    @property
    def pattern_summary(self) -> str:
        """Get a summary of detected patterns."""
        if not self.patterns:
            return "No patterns detected"
        top_patterns = self.patterns[:3]
        pattern_strs = [f"{p.pattern_type.value} ({p.confidence:.0%})" for p in top_patterns]
        return ", ".join(pattern_strs)


class FailureAnalyzer:
    """Main orchestrator for DQ failure analysis.

    Coordinates sample fetching, pattern detection, and strategy recommendation
    to produce a complete analysis of a DQ failure with fix suggestions.
    """

    def __init__(
        self,
        client: OpenMetadataClient,
        registry: StrategyRegistry | None = None,
        fetcher: SampleFetcher | None = None,
        detector: PatternDetector | None = None,
        scorer: ConfidenceScorer | None = None,
    ) -> None:
        """Initialize the failure analyzer.

        Args:
            client: OpenMetadata API client for data fetching.
            registry: Strategy registry (uses default if not provided).
            fetcher: Sample data fetcher (creates new if not provided).
            detector: Pattern detector (creates new if not provided).
            scorer: Confidence scorer (creates new if not provided).
        """
        self.client = client
        self.registry = registry or get_default_registry()
        self.fetcher = fetcher or SampleFetcher()
        self.detector = detector or PatternDetector()
        self.scorer = scorer or ConfidenceScorer()

    async def analyze(
        self,
        test_case_id: str,
        min_confidence: float = 0.4,
    ) -> AnalysisResult:
        """Analyze a DQ failure by test case ID.

        Fetches the test case, sample data, and profile, then performs
        pattern detection and strategy recommendation.

        Args:
            test_case_id: The test case ID or FQN.
            min_confidence: Minimum confidence threshold for recommendations.

        Returns:
            AnalysisResult with full analysis details.

        Raises:
            ValueError: If test case not found.
        """
        start_time = datetime.now(UTC)

        test_case = await self.client.get_test_case_result(test_case_id)
        if test_case is None:
            test_case = await self.client.get_test_case_by_id(test_case_id)

        if test_case is None:
            raise ValueError(f"Test case not found: {test_case_id}")

        fetch_result = await self.fetcher.fetch_for_failure(self.client, test_case)

        context = FailureContext(
            test_case=test_case,
            column_profile=fetch_result.column_profile,
            sample_data=fetch_result.sample_data,
            table_row_count=fetch_result.table_row_count,
        )

        result = await self.analyze_context(context, min_confidence)

        result.analysis_metadata["fetch_errors"] = fetch_result.fetch_errors
        result.analysis_metadata["analysis_duration_ms"] = (
            datetime.now(UTC) - start_time
        ).total_seconds() * 1000

        return result

    async def analyze_context(
        self,
        context: FailureContext,
        min_confidence: float = 0.4,
    ) -> AnalysisResult:
        """Analyze a DQ failure from an existing context.

        Use this method when you already have a FailureContext
        (e.g., from tests or when data is pre-fetched).

        Args:
            context: The failure context with test case and data.
            min_confidence: Minimum confidence threshold for recommendations.

        Returns:
            AnalysisResult with full analysis details.
        """
        start_time = datetime.now(UTC)

        patterns = self.detector.detect_patterns(context)

        test_type = context.test_type or ""
        strategies = self.registry.get_strategies_for_test_type(test_type)
        recommendations = self.scorer.score_multiple_strategies(
            strategies, context, patterns, min_confidence
        )

        best_strategy = recommendations[0] if recommendations else None

        pattern_clarity = self.detector.detect_pattern_clarity(context, patterns)

        return AnalysisResult(
            context=context,
            patterns=patterns,
            recommendations=recommendations,
            best_strategy=best_strategy,
            analysis_metadata={
                "test_type": context.test_type,
                "table_fqn": context.table_fqn,
                "column_name": context.column_name,
                "failed_rows": context.failed_rows,
                "failed_percentage": context.failed_percentage,
                "pattern_clarity": pattern_clarity,
                "strategies_evaluated": len(strategies),
                "strategies_recommended": len(recommendations),
                "analysis_duration_ms": (datetime.now(UTC) - start_time).total_seconds() * 1000,
            },
        )

    async def analyze_multiple(
        self,
        test_case_ids: list[str],
        min_confidence: float = 0.4,
    ) -> list[AnalysisResult]:
        """Analyze multiple DQ failures.

        Args:
            test_case_ids: List of test case IDs to analyze.
            min_confidence: Minimum confidence threshold for recommendations.

        Returns:
            List of AnalysisResults for each test case.
        """
        results: list[AnalysisResult] = []
        for test_case_id in test_case_ids:
            try:
                result = await self.analyze(test_case_id, min_confidence)
                results.append(result)
            except ValueError:
                continue
        return results

    async def get_best_fix(
        self,
        test_case_id: str,
    ) -> tuple[FixStrategy, ConfidenceResult, FailureContext] | None:
        """Get the best fix recommendation for a test case.

        Convenience method that returns just the best strategy
        without the full analysis result.

        Args:
            test_case_id: The test case ID to analyze.

        Returns:
            Tuple of (strategy, confidence, context) or None if no fix found.
        """
        try:
            result = await self.analyze(test_case_id)
            if result.best_strategy:
                return (*result.best_strategy, result.context)
            return None
        except ValueError:
            return None

    def generate_fix_preview(
        self,
        result: AnalysisResult,
        strategy: FixStrategy | None = None,
    ) -> dict[str, Any]:
        """Generate a preview of the fix for an analysis result.

        Args:
            result: The analysis result.
            strategy: Optional specific strategy (uses best if not provided).

        Returns:
            Dictionary with preview information.
        """
        if strategy is None:
            if result.best_strategy is None:
                return {"error": "No recommended strategy available"}
            strategy, confidence = result.best_strategy
        else:
            confidence = self.scorer.score_strategy(strategy, result.context, result.patterns)

        if not strategy.can_apply(result.context):
            return {"error": f"Strategy {strategy.name} cannot be applied"}

        preview = strategy.preview(result.context)
        fix_sql = strategy.generate_fix_sql(result.context)
        rollback_sql = strategy.generate_rollback_sql(result.context)

        return {
            "strategy": strategy.name,
            "strategy_description": strategy.description,
            "confidence": confidence.score,
            "confidence_breakdown": confidence.breakdown,
            "preview": {
                "before_sample": preview.before_sample,
                "after_sample": preview.after_sample,
                "changes_summary": preview.changes_summary,
                "affected_rows": preview.affected_rows,
                "total_rows": preview.total_rows,
                "affected_percentage": preview.affected_percentage,
            },
            "fix_sql": fix_sql,
            "rollback_sql": rollback_sql,
            "patterns": [
                {
                    "type": p.pattern_type.value,
                    "confidence": p.confidence,
                    "affected_count": p.affected_count,
                    "details": p.details,
                }
                for p in result.patterns
            ],
        }
