"""Centralized confidence scoring with pattern awareness."""

from dq_autofix.analyzer.pattern_detector import DetectedPattern, PatternType
from dq_autofix.strategies.base import ConfidenceResult, FailureContext, FixStrategy


class ConfidenceScorer:
    """Centralized confidence scoring with pattern awareness.

    Provides enhanced confidence scoring by incorporating detected patterns
    into the calculation. This scorer wraps strategy-specific confidence
    calculations and adjusts them based on pattern analysis.
    """

    def __init__(
        self,
        coverage_weight: float = 0.25,
        pattern_clarity_weight: float = 0.25,
        reversibility_weight: float = 0.20,
        impact_scope_weight: float = 0.15,
        type_match_weight: float = 0.15,
    ) -> None:
        """Initialize the confidence scorer with custom weights.

        Args:
            coverage_weight: Weight for data coverage factor.
            pattern_clarity_weight: Weight for pattern clarity factor.
            reversibility_weight: Weight for reversibility factor.
            impact_scope_weight: Weight for impact scope factor.
            type_match_weight: Weight for type match factor.
        """
        self.coverage_weight = coverage_weight
        self.pattern_clarity_weight = pattern_clarity_weight
        self.reversibility_weight = reversibility_weight
        self.impact_scope_weight = impact_scope_weight
        self.type_match_weight = type_match_weight

    def score_strategy(
        self,
        strategy: FixStrategy,
        context: FailureContext,
        patterns: list[DetectedPattern] | None = None,
    ) -> ConfidenceResult:
        """Calculate confidence score for a strategy with pattern awareness.

        Combines the strategy's inherent confidence calculation with
        pattern-based adjustments for more accurate scoring.

        Args:
            strategy: The fix strategy to score.
            context: The failure context.
            patterns: Optional list of detected patterns.

        Returns:
            ConfidenceResult with score and breakdown.
        """
        if not strategy.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")

        base_confidence = strategy.calculate_confidence(context)

        if not patterns:
            return base_confidence

        pattern_clarity = self._calculate_pattern_clarity(context, patterns)
        pattern_boost = self._calculate_pattern_boost(strategy, patterns)

        adjusted_score = self._adjust_score_with_patterns(
            base_confidence, pattern_clarity, pattern_boost
        )

        adjusted_breakdown = base_confidence.breakdown.copy()
        adjusted_breakdown["pattern_clarity"] = round(pattern_clarity, 4)
        if pattern_boost != 0:
            adjusted_breakdown["pattern_boost"] = round(pattern_boost, 4)

        reason = base_confidence.reason
        if pattern_boost > 0:
            reason = f"{reason} (boosted by matching patterns)"
        elif pattern_boost < 0:
            reason = f"{reason} (reduced due to conflicting patterns)"

        return ConfidenceResult(
            score=round(adjusted_score, 4),
            breakdown=adjusted_breakdown,
            reason=reason,
        )

    def _calculate_pattern_clarity(
        self,
        context: FailureContext,
        patterns: list[DetectedPattern],
    ) -> float:
        """Calculate pattern clarity factor from detected patterns.

        Higher clarity means we have a clearer understanding of the
        failure pattern, which increases confidence in the fix.

        Args:
            context: The failure context.
            patterns: List of detected patterns.

        Returns:
            Pattern clarity score between 0.0 and 1.0.
        """
        if not patterns:
            return 0.5

        if len(patterns) == 1:
            return patterns[0].confidence

        sorted_patterns = sorted(patterns, key=lambda p: p.confidence, reverse=True)

        weights = [1.0 / (i + 1) for i in range(len(sorted_patterns))]
        confidences = [p.confidence for p in sorted_patterns]

        weighted_sum = sum(c * w for c, w in zip(confidences, weights, strict=False))
        total_weight = sum(weights)

        return min(1.0, weighted_sum / total_weight)

    def _calculate_pattern_boost(
        self,
        strategy: FixStrategy,
        patterns: list[DetectedPattern],
    ) -> float:
        """Calculate confidence boost/penalty based on pattern-strategy match.

        Certain patterns strongly indicate specific strategies should be used.
        This method provides a boost when patterns align with the strategy
        or a penalty when they conflict.

        Args:
            strategy: The fix strategy.
            patterns: List of detected patterns.

        Returns:
            Boost value between -0.15 and 0.15.
        """
        boost = 0.0

        pattern_strategy_affinity = {
            PatternType.WHITESPACE_ISSUES: ["trim_whitespace"],
            PatternType.CASE_INCONSISTENCY: [
                "normalize_case_lower",
                "normalize_case_upper",
                "normalize_case_title",
            ],
            PatternType.SPARSE_NULLS: ["mean_imputation", "median_imputation", "mode_imputation"],
            PatternType.NULL_CLUSTER: ["forward_fill", "mode_imputation"],
            PatternType.DUPLICATE_ROWS: ["keep_first", "keep_last"],
            PatternType.NUMERIC_DISTRIBUTION: ["mean_imputation", "median_imputation"],
        }

        pattern_strategy_conflict = {
            PatternType.OUTLIERS: ["mean_imputation"],
        }

        for pattern in patterns:
            if pattern.pattern_type in pattern_strategy_affinity:
                matching_strategies = pattern_strategy_affinity[pattern.pattern_type]
                if strategy.name in matching_strategies:
                    boost += 0.05 * pattern.confidence

            if pattern.pattern_type in pattern_strategy_conflict:
                conflicting_strategies = pattern_strategy_conflict[pattern.pattern_type]
                if strategy.name in conflicting_strategies:
                    boost -= 0.05 * pattern.confidence

        return max(-0.15, min(0.15, boost))

    def _adjust_score_with_patterns(
        self,
        base_confidence: ConfidenceResult,
        pattern_clarity: float,
        pattern_boost: float,
    ) -> float:
        """Adjust confidence score with pattern information.

        Incorporates pattern clarity into the weighted score calculation
        and applies any pattern-based boost/penalty.

        Args:
            base_confidence: The base confidence from strategy.
            pattern_clarity: Calculated pattern clarity score.
            pattern_boost: Pattern-based boost/penalty.

        Returns:
            Adjusted confidence score between 0.0 and 1.0.
        """
        breakdown = base_confidence.breakdown
        data_coverage = breakdown.get("data_coverage", 0.5)
        reversibility = breakdown.get("reversibility", 0.5)
        impact_scope = breakdown.get("impact_scope", 0.5)
        type_match = breakdown.get("type_match", 0.5)

        score = (
            data_coverage * self.coverage_weight
            + pattern_clarity * self.pattern_clarity_weight
            + reversibility * self.reversibility_weight
            + impact_scope * self.impact_scope_weight
            + type_match * self.type_match_weight
            + pattern_boost
        )

        return max(0.0, min(1.0, score))

    def score_multiple_strategies(
        self,
        strategies: list[FixStrategy],
        context: FailureContext,
        patterns: list[DetectedPattern] | None = None,
        min_confidence: float = 0.4,
    ) -> list[tuple[FixStrategy, ConfidenceResult]]:
        """Score multiple strategies and return sorted results.

        Convenience method to score multiple strategies at once
        and return them sorted by confidence.

        Args:
            strategies: List of strategies to score.
            context: The failure context.
            patterns: Optional list of detected patterns.
            min_confidence: Minimum confidence threshold.

        Returns:
            List of (strategy, confidence) tuples sorted by score descending.
        """
        results: list[tuple[FixStrategy, ConfidenceResult]] = []

        for strategy in strategies:
            confidence = self.score_strategy(strategy, context, patterns)
            if confidence.score >= min_confidence:
                results.append((strategy, confidence))

        results.sort(key=lambda x: x[1].score, reverse=True)
        return results

    def explain_confidence(
        self,
        confidence: ConfidenceResult,
        strategy: FixStrategy,
        patterns: list[DetectedPattern] | None = None,
    ) -> str:
        """Generate a human-readable explanation of the confidence score.

        Args:
            confidence: The confidence result to explain.
            strategy: The strategy being scored.
            patterns: Optional detected patterns.

        Returns:
            Human-readable explanation string.
        """
        lines = [f"Confidence Score: {confidence.score:.1%}"]
        lines.append(f"Strategy: {strategy.name}")
        lines.append("")
        lines.append("Score Breakdown:")

        for factor, value in confidence.breakdown.items():
            factor_name = factor.replace("_", " ").title()
            lines.append(f"  - {factor_name}: {value:.1%}")

        if confidence.reason:
            lines.append("")
            lines.append(f"Reason: {confidence.reason}")

        if patterns:
            lines.append("")
            lines.append("Detected Patterns:")
            for pattern in patterns[:3]:
                lines.append(
                    f"  - {pattern.pattern_type.value}: "
                    f"{pattern.confidence:.1%} confidence, "
                    f"{pattern.affected_count} affected"
                )

        return "\n".join(lines)
