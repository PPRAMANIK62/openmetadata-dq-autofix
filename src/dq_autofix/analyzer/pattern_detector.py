"""Pattern detection for DQ failure analysis."""

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from dq_autofix.strategies.base import FailureContext


class PatternType(StrEnum):
    """Types of patterns that can be detected in data."""

    NULL_CLUSTER = "null_cluster"
    WHITESPACE_ISSUES = "whitespace_issues"
    CASE_INCONSISTENCY = "case_inconsistency"
    DUPLICATE_ROWS = "duplicate_rows"
    FORMAT_MISMATCH = "format_mismatch"
    OUTLIERS = "outliers"
    SPARSE_NULLS = "sparse_nulls"
    NUMERIC_DISTRIBUTION = "numeric_distribution"


@dataclass
class DetectedPattern:
    """A pattern detected in the data.

    Represents a specific data quality pattern found during analysis,
    with confidence level and details about the affected data.
    """

    pattern_type: PatternType
    confidence: float
    affected_count: int
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def is_significant(self) -> bool:
        """Check if pattern is significant enough to act on."""
        return self.confidence >= 0.5 and self.affected_count > 0


class PatternDetector:
    """Detects data patterns that inform fix strategies.

    Analyzes sample data and column profiles to identify patterns
    like null clustering, whitespace issues, case inconsistency, etc.
    These patterns help determine appropriate fix strategies and
    calculate confidence scores.
    """

    def detect_patterns(self, context: FailureContext) -> list[DetectedPattern]:
        """Detect all applicable patterns for a failure context.

        Runs all pattern detectors and returns patterns found.
        The detectors run are based on the test type and available data.

        Args:
            context: The failure context with test case and data.

        Returns:
            List of detected patterns, sorted by confidence descending.
        """
        patterns: list[DetectedPattern] = []

        test_type = context.test_type

        if test_type == "columnValuesToNotBeNull":
            patterns.extend(self._detect_null_patterns(context))
        elif test_type in ("columnValuesToMatchRegex", "columnValuesToBeInSet"):
            patterns.extend(self._detect_string_patterns(context))
        elif test_type == "columnValuesToBeUnique":
            patterns.extend(self._detect_duplicate_patterns(context))

        if context.is_numeric and context.column_profile:
            patterns.extend(self._detect_numeric_patterns(context))

        patterns.sort(key=lambda p: p.confidence, reverse=True)
        return patterns

    def _detect_null_patterns(self, context: FailureContext) -> list[DetectedPattern]:
        """Detect patterns related to null values.

        Args:
            context: The failure context.

        Returns:
            List of null-related patterns.
        """
        patterns: list[DetectedPattern] = []
        null_pct = context.null_percentage

        if null_pct is None:
            return patterns

        if null_pct <= 5:
            patterns.append(
                DetectedPattern(
                    pattern_type=PatternType.SPARSE_NULLS,
                    confidence=0.9,
                    affected_count=context.failed_rows or 0,
                    details={
                        "null_percentage": null_pct,
                        "description": "Low null percentage indicates sparse nulls",
                    },
                )
            )
        elif null_pct <= 20:
            patterns.append(
                DetectedPattern(
                    pattern_type=PatternType.NULL_CLUSTER,
                    confidence=0.7,
                    affected_count=context.failed_rows or 0,
                    details={
                        "null_percentage": null_pct,
                        "description": "Moderate null percentage may indicate clustered nulls",
                    },
                )
            )
        else:
            patterns.append(
                DetectedPattern(
                    pattern_type=PatternType.NULL_CLUSTER,
                    confidence=0.4,
                    affected_count=context.failed_rows or 0,
                    details={
                        "null_percentage": null_pct,
                        "description": "High null percentage - imputation may not be reliable",
                    },
                )
            )

        return patterns

    def _detect_string_patterns(self, context: FailureContext) -> list[DetectedPattern]:
        """Detect patterns in string data (whitespace, case, format).

        Args:
            context: The failure context.

        Returns:
            List of string-related patterns.
        """
        patterns: list[DetectedPattern] = []
        values = context.get_sample_values()

        if not values:
            return patterns

        string_values = [v for v in values if isinstance(v, str)]
        if not string_values:
            return patterns

        whitespace_pattern = self._check_whitespace_issues(string_values)
        if whitespace_pattern:
            patterns.append(whitespace_pattern)

        case_pattern = self._check_case_inconsistency(string_values)
        if case_pattern:
            patterns.append(case_pattern)

        format_pattern = self._check_format_mismatch(string_values)
        if format_pattern:
            patterns.append(format_pattern)

        return patterns

    def _check_whitespace_issues(self, values: list[str]) -> DetectedPattern | None:
        """Check for whitespace issues in string values.

        Args:
            values: List of string values to check.

        Returns:
            DetectedPattern if whitespace issues found, None otherwise.
        """
        issues_count = 0
        for v in values:
            if v != v.strip():
                issues_count += 1

        if issues_count == 0:
            return None

        ratio = issues_count / len(values)
        return DetectedPattern(
            pattern_type=PatternType.WHITESPACE_ISSUES,
            confidence=min(0.95, 0.7 + (ratio * 0.25)),
            affected_count=issues_count,
            details={
                "affected_ratio": ratio,
                "total_checked": len(values),
                "description": f"{issues_count} values have leading/trailing whitespace",
            },
        )

    def _check_case_inconsistency(self, values: list[str]) -> DetectedPattern | None:
        """Check for case inconsistency in string values.

        Args:
            values: List of string values to check.

        Returns:
            DetectedPattern if case inconsistency found, None otherwise.
        """
        lower_count = sum(1 for v in values if v == v.lower())
        upper_count = sum(1 for v in values if v == v.upper())
        title_count = sum(1 for v in values if v == v.title())
        total = len(values)

        max_consistent = max(lower_count, upper_count, title_count)
        inconsistent_count = total - max_consistent

        if inconsistent_count == 0 or max_consistent == total:
            return None

        dominant_case = "lower"
        if upper_count == max_consistent:
            dominant_case = "upper"
        elif title_count == max_consistent:
            dominant_case = "title"

        ratio = inconsistent_count / total
        confidence = 0.6 + (ratio * 0.3) if ratio < 0.5 else 0.5

        return DetectedPattern(
            pattern_type=PatternType.CASE_INCONSISTENCY,
            confidence=confidence,
            affected_count=inconsistent_count,
            details={
                "dominant_case": dominant_case,
                "lower_count": lower_count,
                "upper_count": upper_count,
                "title_count": title_count,
                "inconsistent_ratio": ratio,
                "description": f"Mixed case values, {dominant_case} case is dominant",
            },
        )

    def _check_format_mismatch(self, values: list[str]) -> DetectedPattern | None:
        """Check for format mismatches (dates, emails, etc.).

        Args:
            values: List of string values to check.

        Returns:
            DetectedPattern if format mismatch found, None otherwise.
        """
        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        date_patterns = [
            re.compile(r"^\d{4}-\d{2}-\d{2}$"),
            re.compile(r"^\d{2}/\d{2}/\d{4}$"),
            re.compile(r"^\d{2}-\d{2}-\d{4}$"),
        ]

        email_matches = sum(1 for v in values if email_pattern.match(v))
        if email_matches > len(values) * 0.3:
            non_matches = len(values) - email_matches
            if non_matches > 0:
                return DetectedPattern(
                    pattern_type=PatternType.FORMAT_MISMATCH,
                    confidence=0.7,
                    affected_count=non_matches,
                    details={
                        "format_type": "email",
                        "valid_count": email_matches,
                        "invalid_count": non_matches,
                        "description": f"{non_matches} values don't match email format",
                    },
                )

        for date_pattern in date_patterns:
            date_matches = sum(1 for v in values if date_pattern.match(v))
            if date_matches > len(values) * 0.3:
                non_matches = len(values) - date_matches
                if non_matches > 0:
                    return DetectedPattern(
                        pattern_type=PatternType.FORMAT_MISMATCH,
                        confidence=0.6,
                        affected_count=non_matches,
                        details={
                            "format_type": "date",
                            "valid_count": date_matches,
                            "invalid_count": non_matches,
                            "description": f"{non_matches} values don't match date format",
                        },
                    )

        return None

    def _detect_duplicate_patterns(self, context: FailureContext) -> list[DetectedPattern]:
        """Detect patterns related to duplicate values.

        Args:
            context: The failure context.

        Returns:
            List of duplicate-related patterns.
        """
        patterns: list[DetectedPattern] = []
        failed_rows = context.failed_rows or 0

        if failed_rows == 0:
            return patterns

        failed_pct = context.failed_percentage or 0

        if failed_pct <= 1:
            confidence = 0.85
            description = "Very few duplicates, likely data entry errors"
        elif failed_pct <= 5:
            confidence = 0.75
            description = "Small duplicate rate, may be intentional or errors"
        else:
            confidence = 0.5
            description = "High duplicate rate, investigate root cause"

        patterns.append(
            DetectedPattern(
                pattern_type=PatternType.DUPLICATE_ROWS,
                confidence=confidence,
                affected_count=failed_rows,
                details={
                    "duplicate_percentage": failed_pct,
                    "description": description,
                },
            )
        )

        return patterns

    def _detect_numeric_patterns(self, context: FailureContext) -> list[DetectedPattern]:
        """Detect patterns in numeric data (distribution, outliers).

        Args:
            context: The failure context.

        Returns:
            List of numeric-related patterns.
        """
        patterns: list[DetectedPattern] = []
        profile = context.column_profile

        if not profile:
            return patterns

        if profile.mean is not None and profile.std_dev is not None and profile.std_dev > 0:
            cv = profile.std_dev / abs(profile.mean) if profile.mean != 0 else float("inf")
            if cv > 1.0:
                patterns.append(
                    DetectedPattern(
                        pattern_type=PatternType.OUTLIERS,
                        confidence=0.6,
                        affected_count=0,
                        details={
                            "coefficient_of_variation": cv,
                            "mean": profile.mean,
                            "std_dev": profile.std_dev,
                            "description": "High variance suggests possible outliers",
                        },
                    )
                )

        if profile.mean is not None and profile.median is not None:
            skewness_indicator = (
                (profile.mean - profile.median) / profile.std_dev if profile.std_dev else 0
            )
            distribution_type = "normal"
            if abs(skewness_indicator) > 0.5:
                distribution_type = "right_skewed" if skewness_indicator > 0 else "left_skewed"

            patterns.append(
                DetectedPattern(
                    pattern_type=PatternType.NUMERIC_DISTRIBUTION,
                    confidence=0.7,
                    affected_count=0,
                    details={
                        "distribution_type": distribution_type,
                        "mean": profile.mean,
                        "median": profile.median,
                        "skewness_indicator": skewness_indicator,
                        "description": f"Distribution appears to be {distribution_type}",
                    },
                )
            )

        return patterns

    def detect_pattern_clarity(
        self, context: FailureContext, patterns: list[DetectedPattern]
    ) -> float:
        """Calculate overall pattern clarity score.

        Combines individual pattern confidences into a single clarity score
        that represents how well we understand the failure pattern.

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

        confidences = [p.confidence for p in patterns]
        weights = [1.0 / (i + 1) for i in range(len(confidences))]
        weighted_sum = sum(c * w for c, w in zip(confidences, weights, strict=False))
        total_weight = sum(weights)

        return min(1.0, weighted_sum / total_weight)
