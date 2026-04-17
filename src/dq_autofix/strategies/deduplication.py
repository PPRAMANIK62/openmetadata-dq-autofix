"""Deduplication strategies for handling duplicate values."""

from abc import abstractmethod
from collections import Counter, defaultdict
from typing import Any, ClassVar

from dq_autofix.strategies.base import (
    ConfidenceResult,
    FailureContext,
    FixStrategy,
    PreviewResult,
)


class BaseDeduplicationStrategy(FixStrategy):
    """Base class for deduplication strategies.

    Provides common functionality for KeepFirst and KeepLast strategies.
    Subclasses only need to define:
    - name, description
    - _get_preferred_columns(): Order of preference for ordering column
    - _get_agg_func(): "MIN" or "MAX" for SQL
    - _get_sort_order(): "ASC" or "DESC" for SQL
    - _get_type_match(): Scoring logic for ordering column
    - _sort_reverse: Whether to reverse sort in preview
    """

    supported_test_types: ClassVar[list[str]] = ["columnValuesToBeUnique"]
    reversibility_score: ClassVar[float] = 0.0

    def __init__(self, order_column: str = "id"):
        """Initialize with ordering column.

        Args:
            order_column: Column to use for ordering.
        """
        self.order_column = order_column

    @property
    @abstractmethod
    def _sort_reverse(self) -> bool:
        """Whether to reverse sort in preview (False = keep first, True = keep last)."""

    @abstractmethod
    def _get_preferred_columns(self) -> list[str]:
        """Get preferred column ordering for detecting order column."""

    @abstractmethod
    def _get_agg_func(self) -> str:
        """Get SQL aggregate function: 'MIN' for first, 'MAX' for last."""

    @abstractmethod
    def _get_sort_order(self) -> str:
        """Get SQL sort order: 'ASC' for first, 'DESC' for last."""

    @abstractmethod
    def _get_type_match(self, order_col: str) -> float:
        """Calculate type match score based on ordering column name."""

    @abstractmethod
    def _get_keep_description(self) -> str:
        """Get description for 'keep first' or 'keep last (most recent)'."""

    def can_apply(self, context: FailureContext) -> bool:
        """Check if deduplication is applicable."""
        if context.test_type not in self.supported_test_types:
            return False
        return bool(context.column_name)

    def _detect_order_column(self, context: FailureContext) -> str:
        """Detect best ordering column from sample data."""
        if not context.sample_data:
            return self.order_column

        columns = context.sample_data.columns
        for col in self._get_preferred_columns():
            if col in columns:
                return col

        return columns[0] if columns else self.order_column

    def _count_duplicates(self, context: FailureContext) -> tuple[int, list[Any]]:
        """Count duplicate values and return examples.

        Returns (duplicate_count, list of duplicate values).
        """
        values = context.get_sample_values()
        if not values:
            return 0, []

        counts = Counter(v for v in values if v is not None)
        duplicates = [(v, c) for v, c in counts.items() if c > 1]
        dup_count = sum(c - 1 for _, c in duplicates)
        dup_values = [v for v, _ in duplicates[:5]]

        return dup_count, dup_values

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        """Calculate confidence for deduplication."""
        if result := self._check_applicability(context):
            return result

        dup_count, dup_values = self._count_duplicates(context)
        order_col = self._detect_order_column(context)

        data_coverage = self._get_data_coverage(context)
        pattern_clarity = min(1.0, 0.7 + dup_count / 100) if dup_count > 0 else 0.5
        impact_scope = self._get_impact_scope_from_failed_pct(context)
        type_match = self._get_type_match(order_col)

        reason = (
            f"Keep {self._get_keep_description()} by '{order_col}' ({dup_count} duplicates found)"
        )
        if dup_values:
            reason += f", e.g.: {dup_values[:3]}"

        return self._build_confidence(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )

    def generate_fix_sql(self, context: FailureContext) -> str:
        """Generate SQL to delete duplicates."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        order_col = self._detect_order_column(context)
        agg_func = self._get_agg_func()
        sort_order = self._get_sort_order()

        return f"""-- Delete duplicates keeping the {self._get_keep_description()} occurrence by {order_col}
DELETE FROM {table}
WHERE {self._quote_identifier(order_col)} NOT IN (
    SELECT {agg_func}({self._quote_identifier(order_col)})
    FROM {table}
    GROUP BY {self._quote_identifier(column)}
);

-- Alternative using CTE (for databases that support it):
-- WITH duplicates AS (
--     SELECT {self._quote_identifier(order_col)},
--            ROW_NUMBER() OVER (
--                PARTITION BY {self._quote_identifier(column)}
--                ORDER BY {self._quote_identifier(order_col)} {sort_order}
--            ) AS rn
--     FROM {table}
-- )
-- DELETE FROM {table}
-- WHERE {self._quote_identifier(order_col)} IN (
--     SELECT {self._quote_identifier(order_col)} FROM duplicates WHERE rn > 1
-- );"""

    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        """Generate backup SQL - deletions are not reversible without backup."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        table_name = self._get_table_name(context)

        return f"""-- IMPORTANT: Backup ALL data before deleting duplicates!
-- Deletions cannot be undone without a backup.

CREATE TABLE {table_name}_backup_full AS
SELECT * FROM {table};

-- Or backup only duplicate rows:
CREATE TABLE {table_name}_backup_duplicates AS
SELECT * FROM {table}
WHERE {self._quote_identifier(column)} IN (
    SELECT {self._quote_identifier(column)}
    FROM {table}
    GROUP BY {self._quote_identifier(column)}
    HAVING COUNT(*) > 1
);"""

    def preview(self, context: FailureContext) -> PreviewResult:
        """Generate preview of deduplication."""
        column = context.column_name
        order_col = self._detect_order_column(context)

        before_sample: list[dict[str, Any]] = []
        after_sample: list[dict[str, Any]] = []

        if context.sample_data and column:
            rows_by_value: dict[Any, list[dict[str, Any]]] = defaultdict(list)

            for row_dict in context.sample_data.to_dicts():
                value = row_dict.get(column)
                if value is not None:
                    rows_by_value[value].append(row_dict)

            for _value, rows in rows_by_value.items():
                if len(rows) > 1:
                    sorted_rows = sorted(
                        rows, key=lambda r: r.get(order_col, 0), reverse=self._sort_reverse
                    )
                    for dup_row in sorted_rows[1:]:
                        before_sample.append(dup_row)
                        if len(before_sample) >= 5:
                            break
                if len(before_sample) >= 5:
                    break

        dup_count, _ = self._count_duplicates(context)
        affected = context.failed_rows or dup_count
        total = context.table_row_count

        return PreviewResult(
            before_sample=before_sample,
            after_sample=after_sample,
            changes_summary=f"Delete {affected} duplicate rows (keeping {self._get_keep_description()} by {order_col})",
            affected_rows=affected,
            total_rows=total,
        )


class KeepFirstStrategy(BaseDeduplicationStrategy):
    """Remove duplicates by keeping the first occurrence.

    Uses ordering column to determine "first". Data loss is expected.
    """

    name = "keep_first"
    description = "Remove duplicates by keeping the first occurrence"

    @property
    def _sort_reverse(self) -> bool:
        return False

    def _get_preferred_columns(self) -> list[str]:
        return ["id", "created_at", "timestamp", "inserted_at", "row_id"]

    def _get_agg_func(self) -> str:
        return "MIN"

    def _get_sort_order(self) -> str:
        return "ASC"

    def _get_keep_description(self) -> str:
        return "first"

    def _get_type_match(self, order_col: str) -> float:
        time_cols = {"created_at", "timestamp", "inserted_at", "updated_at", "date"}
        return 1.0 if order_col.lower() in time_cols or order_col.lower() == "id" else 0.7


class KeepLastStrategy(BaseDeduplicationStrategy):
    """Remove duplicates by keeping the last occurrence.

    Uses ordering column to determine "last". Data loss is expected.
    """

    name = "keep_last"
    description = "Remove duplicates by keeping the last (most recent) occurrence"

    @property
    def _sort_reverse(self) -> bool:
        return True

    def _get_preferred_columns(self) -> list[str]:
        return ["updated_at", "modified_at", "timestamp", "created_at", "id"]

    def _get_agg_func(self) -> str:
        return "MAX"

    def _get_sort_order(self) -> str:
        return "DESC"

    def _get_keep_description(self) -> str:
        return "last"

    def _get_type_match(self, order_col: str) -> float:
        update_cols = {"updated_at", "modified_at", "last_modified"}
        if order_col.lower() in update_cols:
            return 1.0
        if order_col.lower() in {"created_at", "timestamp", "id"}:
            return 0.9
        return 0.7
