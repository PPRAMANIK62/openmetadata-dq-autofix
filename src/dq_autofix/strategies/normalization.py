"""Normalization strategies for fixing format and case issues."""

from abc import abstractmethod
from collections.abc import Callable
from typing import ClassVar

from dq_autofix.preview import DiffGenerator
from dq_autofix.strategies.base import (
    CaseType,
    ConfidenceResult,
    FailureContext,
    FixStrategy,
    PreviewResult,
)


class BaseNormalizationStrategy(FixStrategy):
    """Base class for string normalization strategies.

    Provides common implementation for can_apply, calculate_confidence, preview.
    Subclasses define:
    - _count_issues(): Count values needing normalization
    - _transform_value(): Apply transformation to a value
    - _get_change_description(): Description for summary
    - generate_fix_sql(): SQL varies per strategy
    - generate_rollback_sql(): Backup SQL varies per strategy
    """

    supported_test_types: ClassVar[list[str]] = [
        "columnValuesToMatchRegex",
        "columnValuesToBeInSet",
    ]

    def can_apply(self, context: FailureContext) -> bool:
        """Check if normalization is applicable."""
        if context.test_type not in self.supported_test_types:
            return False
        return bool(context.column_name)

    @abstractmethod
    def _count_issues(self, context: FailureContext) -> int:
        """Count values that need normalization."""

    @abstractmethod
    def _should_transform(self, value: str) -> bool:
        """Check if a value should be transformed."""

    @abstractmethod
    def _transform_value(self, value: str) -> str:
        """Apply the transformation to a value."""

    @abstractmethod
    def _get_change_description(self, affected_count: int) -> str:
        """Get description for changes summary."""

    def _get_impact_scope(self) -> float:
        """Get impact scope for confidence calculation. Override if needed."""
        return 0.95

    def _get_type_match(self, context: FailureContext) -> float:
        """Get type match score. Override if strategy-specific logic needed."""
        return 1.0

    def _get_extra_reason_info(self, context: FailureContext) -> str:
        """Get extra info to append to reason. Override if needed."""
        return ""

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        """Calculate confidence for normalization."""
        if result := self._check_applicability(context):
            return result

        issue_count = self._count_issues(context)
        values = context.get_sample_values()
        total = len([v for v in values if v is not None])

        if total == 0:
            pattern_clarity = 0.5
        else:
            pattern_clarity = min(1.0, (issue_count / total) * 2) if issue_count > 0 else 0.3

        data_coverage = self._get_data_coverage(context, base=0.9)
        impact_scope = self._get_impact_scope()
        type_match = self._get_type_match(context)

        reason = self._get_change_description(issue_count)
        extra = self._get_extra_reason_info(context)
        if extra:
            reason += f" ({extra})"

        return self._build_confidence(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )

    def preview(self, context: FailureContext) -> PreviewResult:
        """Generate preview of normalization."""
        column = context.column_name
        assert column is not None

        diff = DiffGenerator.build_sample_diff(
            context,
            column=column,
            should_include=lambda v: isinstance(v, str) and self._should_transform(v),
            transform=self._transform_value,
        )

        return DiffGenerator.build_preview_result(
            diff,
            changes_summary=self._get_change_description(len(diff.before)),
            affected_rows=len(diff.before),
            total_rows=context.table_row_count,
        )


class TrimWhitespaceStrategy(BaseNormalizationStrategy):
    """Remove leading and trailing whitespace from string values.

    This is a lossless transformation with very high confidence.
    """

    name = "trim_whitespace"
    description = "Remove leading and trailing whitespace from values"
    reversibility_score: ClassVar[float] = 1.0

    def _count_issues(self, context: FailureContext) -> int:
        """Count values with leading/trailing whitespace."""
        values = context.get_sample_values()
        count = 0
        for v in values:
            if isinstance(v, str) and v != v.strip():
                count += 1
        return count

    def _should_transform(self, value: str) -> bool:
        return value != value.strip()

    def _transform_value(self, value: str) -> str:
        return value.strip()

    def _get_change_description(self, affected_count: int) -> str:
        return f"Trim whitespace from {affected_count} values"

    def generate_fix_sql(self, context: FailureContext) -> str:
        """Generate UPDATE SQL to trim whitespace."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None

        return f"""UPDATE {table}
SET {self._quote_identifier(column)} = TRIM({self._quote_identifier(column)})
WHERE {self._quote_identifier(column)} != TRIM({self._quote_identifier(column)});"""

    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        """Trim is not reversible - whitespace is lost."""
        column = context.column_name
        assert column is not None
        where = f"{self._quote_identifier(column)} != TRIM({self._quote_identifier(column)})"
        return self._build_backup_sql(
            context,
            suffix="whitespace",
            where_clause=where,
            comment="Backup affected rows before applying fix (whitespace cannot be restored):",
        )


class NormalizeCaseStrategy(BaseNormalizationStrategy):
    """Normalize string case to lower, upper, or title case.

    Useful when values fail regex or set membership due to case differences.
    """

    name = "normalize_case"
    description = "Normalize string case (lower, upper, or title)"
    reversibility_score: ClassVar[float] = 0.9

    def __init__(self, case_type: CaseType = CaseType.LOWER):
        """Initialize with target case type.

        Args:
            case_type: The case to normalize to (lower, upper, title)
        """
        self.case_type = case_type
        self._case_transforms: dict[CaseType, Callable[[str], str]] = {
            CaseType.LOWER: str.lower,
            CaseType.UPPER: str.upper,
            CaseType.TITLE: str.title,
        }

    def _detect_case_issues(self, context: FailureContext) -> tuple[int, str]:
        """Detect case inconsistencies in sample data.

        Returns (count of issues, detected predominant case).
        """
        values = context.get_sample_values()
        string_values = [v for v in values if isinstance(v, str) and v]

        if not string_values:
            return 0, "unknown"

        lower_count = sum(1 for v in string_values if v == v.lower())
        upper_count = sum(1 for v in string_values if v == v.upper())
        title_count = sum(1 for v in string_values if v == v.title())

        total = len(string_values)
        mixed_count = total - max(lower_count, upper_count, title_count)

        if lower_count == total:
            predominant = "lower"
        elif upper_count == total:
            predominant = "upper"
        elif title_count == total:
            predominant = "title"
        else:
            predominant = "mixed"

        return mixed_count, predominant

    def _count_issues(self, context: FailureContext) -> int:
        mixed_count, _ = self._detect_case_issues(context)
        return mixed_count

    def _should_transform(self, value: str) -> bool:
        if not value:
            return False
        transform = self._case_transforms[self.case_type]
        return value != transform(value)

    def _transform_value(self, value: str) -> str:
        return self._case_transforms[self.case_type](value)

    def _get_change_description(self, affected_count: int) -> str:
        return f"Normalize {affected_count} values to {self.case_type.value} case"

    def _get_impact_scope(self) -> float:
        return 0.9

    def _get_type_match(self, context: FailureContext) -> float:
        _, predominant = self._detect_case_issues(context)
        return 1.0 if predominant != "unknown" else 0.7

    def _get_extra_reason_info(self, context: FailureContext) -> str:
        issue_count, _ = self._detect_case_issues(context)
        return f"{issue_count} inconsistent values"

    def generate_fix_sql(self, context: FailureContext) -> str:
        """Generate UPDATE SQL to normalize case."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None

        case_func = {
            CaseType.LOWER: "LOWER",
            CaseType.UPPER: "UPPER",
            CaseType.TITLE: "INITCAP",
        }[self.case_type]

        return f"""UPDATE {table}
SET {self._quote_identifier(column)} = {case_func}({self._quote_identifier(column)})
WHERE {self._quote_identifier(column)} IS NOT NULL;"""

    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        """Generate backup SQL - original case cannot be restored without backup."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        table_name = self._get_table_name(context)

        return f"""-- Backup original values before applying fix:
CREATE TABLE {table_name}_backup_case AS
SELECT * FROM {table}
WHERE {self._quote_identifier(column)} IS NOT NULL;

-- To restore original case:
-- UPDATE {table} t
-- SET {self._quote_identifier(column)} = b.{self._quote_identifier(column)}
-- FROM {table_name}_backup_case b
-- WHERE t.id = b.id;  -- Adjust join condition as needed"""
