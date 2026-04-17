"""Base classes and interfaces for fix strategies."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar

from dq_autofix.openmetadata.models import ColumnProfile, SampleData, TestCaseResult


class CaseType(StrEnum):
    """Case normalization types."""

    LOWER = "lower"
    UPPER = "upper"
    TITLE = "title"


@dataclass
class FailureContext:
    """Context for analyzing a DQ failure and generating fixes.

    Holds all the information needed by strategies to analyze a failure
    and generate appropriate fix SQL.
    """

    test_case: TestCaseResult
    column_profile: ColumnProfile | None = None
    sample_data: SampleData | None = None
    table_row_count: int | None = None

    @property
    def table_fqn(self) -> str:
        """Get the fully qualified table name."""
        return self.test_case.table_fqn

    @property
    def column_name(self) -> str | None:
        """Get the column name if applicable."""
        return self.test_case.column_name

    @property
    def test_type(self) -> str:
        """Get the test definition type."""
        return self.test_case.test_definition

    @property
    def failed_rows(self) -> int | None:
        """Get the number of failed rows."""
        if self.test_case.result:
            return self.test_case.result.failed_rows
        return None

    @property
    def failed_percentage(self) -> float | None:
        """Get the percentage of failed rows."""
        if self.test_case.result:
            return self.test_case.result.failed_rows_percentage
        return None

    @property
    def null_percentage(self) -> float | None:
        """Get null percentage from column profile."""
        if self.column_profile and self.column_profile.null_proportion is not None:
            return self.column_profile.null_proportion * 100
        return None

    @property
    def is_numeric(self) -> bool:
        """Check if column is numeric type."""
        if not self.column_profile or not self.column_profile.data_type:
            return False
        numeric_types = {
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "NUMERIC",
            "NUMBER",
            "REAL",
        }
        return self.column_profile.data_type.upper() in numeric_types

    def get_sample_values(self, column: str | None = None) -> list[Any]:
        """Get sample values for a column."""
        if not self.sample_data:
            return []
        col = column or self.column_name
        if not col:
            return []
        try:
            col_idx = self.sample_data.columns.index(col)
            return [row[col_idx] for row in self.sample_data.rows]
        except (ValueError, IndexError):
            return []


@dataclass
class ConfidenceResult:
    """Result of confidence calculation for a fix strategy.

    Confidence is scored 0.0-1.0 based on weighted factors:
    - data_coverage (0.25): % of data we can analyze
    - pattern_clarity (0.25): How clear is the failure pattern
    - reversibility (0.20): Can we undo this fix?
    - impact_scope (0.15): % of rows affected (fewer = higher confidence)
    - type_match (0.15): Does strategy match data type
    """

    score: float
    breakdown: dict[str, float] = field(default_factory=dict)
    reason: str = ""

    HIGH_THRESHOLD = 0.80
    MEDIUM_THRESHOLD = 0.60
    LOW_THRESHOLD = 0.40

    @property
    def is_high(self) -> bool:
        """Check if confidence is high (>=80%)."""
        return self.score >= self.HIGH_THRESHOLD

    @property
    def is_medium(self) -> bool:
        """Check if confidence is medium (60-80%)."""
        return self.MEDIUM_THRESHOLD <= self.score < self.HIGH_THRESHOLD

    @property
    def is_low(self) -> bool:
        """Check if confidence is low (40-60%)."""
        return self.LOW_THRESHOLD <= self.score < self.MEDIUM_THRESHOLD

    @property
    def should_skip(self) -> bool:
        """Check if confidence is too low to suggest."""
        return self.score < self.LOW_THRESHOLD

    @classmethod
    def calculate(
        cls,
        data_coverage: float,
        pattern_clarity: float,
        reversibility: float,
        impact_scope: float,
        type_match: float,
        reason: str = "",
    ) -> "ConfidenceResult":
        """Calculate weighted confidence score.

        All inputs should be 0.0-1.0.
        """
        score = (
            data_coverage * 0.25
            + pattern_clarity * 0.25
            + reversibility * 0.20
            + impact_scope * 0.15
            + type_match * 0.15
        )
        return cls(
            score=round(score, 4),
            breakdown={
                "data_coverage": round(data_coverage, 4),
                "pattern_clarity": round(pattern_clarity, 4),
                "reversibility": round(reversibility, 4),
                "impact_scope": round(impact_scope, 4),
                "type_match": round(type_match, 4),
            },
            reason=reason,
        )


@dataclass
class PreviewResult:
    """Preview of a fix showing before/after samples.

    Provides a visual diff of what the fix will do to sample data.
    """

    before_sample: list[dict[str, Any]]
    after_sample: list[dict[str, Any]]
    changes_summary: str
    affected_rows: int
    total_rows: int | None = None

    @property
    def affected_percentage(self) -> float | None:
        """Calculate percentage of affected rows."""
        if self.total_rows and self.total_rows > 0:
            return round((self.affected_rows / self.total_rows) * 100, 2)
        return None


class FixStrategy(ABC):
    """Abstract base class for fix strategies.

    Each strategy targets specific test types and generates SQL to fix
    data quality issues. Strategies are stateless - all context is passed
    per method call.
    """

    name: ClassVar[str]
    description: ClassVar[str]
    supported_test_types: ClassVar[list[str]]
    reversibility_score: ClassVar[float] = 0.5

    @abstractmethod
    def can_apply(self, context: FailureContext) -> bool:
        """Check if this strategy can be applied to the given failure.

        Args:
            context: The failure context with test case and profile info.

        Returns:
            True if this strategy is applicable.
        """

    @abstractmethod
    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        """Calculate confidence score for applying this strategy.

        Args:
            context: The failure context with test case and profile info.

        Returns:
            Confidence result with score and breakdown.
        """

    @abstractmethod
    def generate_fix_sql(self, context: FailureContext) -> str:
        """Generate SQL to fix the data quality issue.

        Args:
            context: The failure context with test case and profile info.

        Returns:
            SQL statement(s) to apply the fix.
        """

    @abstractmethod
    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        """Generate SQL to rollback/undo the fix if possible.

        Args:
            context: The failure context with test case and profile info.

        Returns:
            SQL statement(s) to rollback, or None if not reversible.
        """

    @abstractmethod
    def preview(self, context: FailureContext) -> PreviewResult:
        """Generate a preview of what the fix will do.

        Args:
            context: The failure context with test case and profile info.

        Returns:
            Preview showing before/after samples.
        """

    # -------------------------------------------------------------------------
    # Helper methods for common patterns across strategies
    # -------------------------------------------------------------------------

    def _get_table_name(self, context: FailureContext) -> str:
        """Extract simple table name for SQL."""
        fqn = context.table_fqn
        parts = fqn.split(".")
        return parts[-1] if parts else fqn

    def _get_full_table_ref(self, context: FailureContext) -> str:
        """Get full table reference for SQL (schema.table or just table)."""
        fqn = context.table_fqn
        parts = fqn.split(".")
        if len(parts) >= 2:
            return f"{parts[-2]}.{parts[-1]}"
        return parts[-1] if parts else fqn

    def _quote_identifier(self, name: str) -> str:
        """Quote an identifier for SQL."""
        return f'"{name}"'

    def _check_applicability(self, context: FailureContext) -> ConfidenceResult | None:
        """Return early ConfidenceResult if strategy is not applicable.

        Use at the start of calculate_confidence() to reduce boilerplate:

            if result := self._check_applicability(context):
                return result

        Returns:
            ConfidenceResult with score 0 if not applicable, None otherwise.
        """
        if not self.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")
        return None

    def _get_data_coverage(
        self, context: FailureContext, base: float = 0.8, fallback: float = 0.5
    ) -> float:
        """Calculate data coverage factor based on sample data availability.

        Args:
            context: The failure context.
            base: Score when sample data is available (default 0.8).
            fallback: Score when sample data is not available (default 0.5).

        Returns:
            Data coverage score between 0.0 and 1.0.
        """
        return base if context.sample_data else fallback

    def _get_data_coverage_from_profile(
        self, context: FailureContext, base: float = 1.0, fallback: float = 0.5
    ) -> float:
        """Calculate data coverage from column profile values_count.

        Args:
            context: The failure context.
            base: Score when profile has values_count (default 1.0).
            fallback: Score when profile is missing or empty (default 0.5).

        Returns:
            Data coverage score between 0.0 and 1.0.
        """
        if context.column_profile and context.column_profile.values_count:
            return base
        return fallback

    def _get_impact_scope_from_null_pct(self, context: FailureContext) -> float:
        """Calculate impact scope based on null percentage.

        Lower null percentage = higher confidence (smaller impact).

        Returns:
            Impact scope score between 0.0 and 1.0.
        """
        null_pct = context.null_percentage or 0
        return max(0.0, 1.0 - (null_pct / 100)) if null_pct else 0.9

    def _get_impact_scope_from_failed_pct(
        self, context: FailureContext, min_score: float = 0.3, divisor: float = 50
    ) -> float:
        """Calculate impact scope based on failed percentage.

        Lower failed percentage = higher confidence (smaller impact).

        Args:
            context: The failure context.
            min_score: Minimum score to return (default 0.3).
            divisor: Divisor for percentage calculation (default 50).

        Returns:
            Impact scope score between min_score and 1.0.
        """
        failed_pct = context.failed_percentage or 0
        return max(min_score, 1.0 - (failed_pct / divisor))

    def _build_confidence(
        self,
        data_coverage: float,
        pattern_clarity: float,
        impact_scope: float,
        type_match: float,
        reason: str,
    ) -> ConfidenceResult:
        """Build a ConfidenceResult using the strategy's reversibility_score.

        Convenience method to avoid repeating `reversibility=self.reversibility_score`.

        Args:
            data_coverage: How much data we can analyze (0.0-1.0).
            pattern_clarity: How clear the failure pattern is (0.0-1.0).
            impact_scope: Proportion of rows affected (0.0-1.0, higher = fewer rows).
            type_match: How well strategy matches data type (0.0-1.0).
            reason: Human-readable reason for the confidence score.

        Returns:
            ConfidenceResult with weighted score.
        """
        return ConfidenceResult.calculate(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            reversibility=self.reversibility_score,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )

    def _build_backup_sql(
        self, context: FailureContext, suffix: str, where_clause: str, comment: str = ""
    ) -> str:
        """Generate standardized backup table SQL.

        Args:
            context: The failure context.
            suffix: Suffix for backup table name (e.g., 'nulls', 'duplicates').
            where_clause: WHERE clause to filter rows to backup.
            comment: Optional comment to prepend.

        Returns:
            CREATE TABLE AS SELECT SQL statement.
        """
        table = self._get_full_table_ref(context)
        table_name = self._get_table_name(context)
        comment_line = f"-- {comment}\n" if comment else ""
        return f"""{comment_line}CREATE TABLE {table_name}_backup_{suffix} AS
SELECT * FROM {table}
WHERE {where_clause};"""
