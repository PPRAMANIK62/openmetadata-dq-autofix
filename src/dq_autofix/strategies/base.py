"""Base classes and interfaces for fix strategies."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

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

    name: str
    description: str
    supported_test_types: list[str]
    reversibility_score: float = 0.5

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
