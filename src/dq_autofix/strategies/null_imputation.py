"""Null imputation strategies for handling NULL values."""

from abc import abstractmethod
from typing import Any, ClassVar

from dq_autofix.strategies.base import (
    ConfidenceResult,
    FailureContext,
    FixStrategy,
    PreviewResult,
)


class BaseNullImputationStrategy(FixStrategy):
    """Base class for null imputation strategies.

    Provides common implementation for generate_rollback_sql, preview,
    and parts of generate_fix_sql and calculate_confidence.
    """

    supported_test_types: ClassVar[list[str]] = ["columnValuesToNotBeNull"]

    @abstractmethod
    def _get_imputation_value(self, context: FailureContext) -> Any:
        """Get the value to use for imputation.

        Returns:
            The value to replace NULLs with.
        """

    @abstractmethod
    def _get_value_description(self, value: Any) -> str:
        """Get a human-readable description of the imputation value.

        Args:
            value: The imputation value.

        Returns:
            Description like "mean (45.5)" or "mode 'pending'".
        """

    def _format_sql_value(self, value: Any) -> str:
        """Format a value for SQL.

        Args:
            value: The value to format.

        Returns:
            SQL-safe string representation.
        """
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)

    def generate_fix_sql(self, context: FailureContext) -> str:
        """Generate UPDATE SQL to replace NULLs with imputation value."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        value = self._get_imputation_value(context)
        sql_value = self._format_sql_value(value)

        return f"""UPDATE {table}
SET {self._quote_identifier(column)} = {sql_value}
WHERE {self._quote_identifier(column)} IS NULL;"""

    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        """Generate backup SQL - imputation is not perfectly reversible."""
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        table_name = self._get_table_name(context)

        return f"""-- Backup affected rows before applying fix:
CREATE TABLE {table_name}_backup_nulls AS
SELECT * FROM {table}
WHERE {self._quote_identifier(column)} IS NULL;"""

    def preview(self, context: FailureContext) -> PreviewResult:
        """Generate preview of imputation."""
        value = self._get_imputation_value(context)
        column = context.column_name

        before_sample: list[dict[str, Any]] = []
        after_sample: list[dict[str, Any]] = []

        if context.sample_data and column:
            for row_dict in context.sample_data.to_dicts():
                if row_dict.get(column) is None:
                    before_sample.append(row_dict.copy())
                    after_row = row_dict.copy()
                    after_row[column] = value
                    after_sample.append(after_row)
                    if len(before_sample) >= 5:
                        break

        affected = context.failed_rows or 0
        total = context.table_row_count
        value_desc = self._get_value_description(value)

        return PreviewResult(
            before_sample=before_sample,
            after_sample=after_sample,
            changes_summary=f"Replace {affected} NULL values with {value_desc}",
            affected_rows=affected,
            total_rows=total,
        )


class MeanImputationStrategy(BaseNullImputationStrategy):
    """Replace NULL values with the column mean.

    Best for numeric columns with normal distribution and low null percentage.
    """

    name = "mean_imputation"
    description = "Replace NULL values with the column mean (average)"
    reversibility_score = 0.5

    def can_apply(self, context: FailureContext) -> bool:
        if context.test_type not in self.supported_test_types:
            return False
        if not context.is_numeric:
            return False
        return context.column_profile is not None and context.column_profile.mean is not None

    def _get_imputation_value(self, context: FailureContext) -> Any:
        assert context.column_profile is not None
        return context.column_profile.mean

    def _get_value_description(self, value: Any) -> str:
        return f"mean ({value})"

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        if not self.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")

        profile = context.column_profile
        assert profile is not None

        data_coverage = 1.0 if profile.values_count else 0.5
        null_pct = context.null_percentage or 0
        pattern_clarity = max(0.0, 1.0 - (null_pct / 50))
        reversibility = self.reversibility_score
        impact_scope = max(0.0, 1.0 - (null_pct / 100)) if null_pct else 0.9
        type_match = 1.0

        reason = f"Mean imputation for {null_pct:.1f}% null values"
        if null_pct > 20:
            reason += " (high null % reduces confidence)"

        return ConfidenceResult.calculate(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            reversibility=reversibility,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )


class MedianImputationStrategy(BaseNullImputationStrategy):
    """Replace NULL values with the column median.

    Better than mean when data has outliers or is skewed.
    """

    name = "median_imputation"
    description = "Replace NULL values with the column median"
    reversibility_score = 0.5

    def can_apply(self, context: FailureContext) -> bool:
        if context.test_type not in self.supported_test_types:
            return False
        if not context.is_numeric:
            return False
        return context.column_profile is not None and context.column_profile.median is not None

    def _get_imputation_value(self, context: FailureContext) -> Any:
        assert context.column_profile is not None
        return context.column_profile.median

    def _get_value_description(self, value: Any) -> str:
        return f"median ({value})"

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        if not self.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")

        profile = context.column_profile
        assert profile is not None

        data_coverage = 1.0 if profile.values_count else 0.5
        null_pct = context.null_percentage or 0
        pattern_clarity = max(0.0, 1.0 - (null_pct / 50))
        reversibility = self.reversibility_score
        impact_scope = max(0.0, 1.0 - (null_pct / 100)) if null_pct else 0.9
        type_match = 1.0

        # Boost confidence if outliers detected (high coefficient of variation)
        if profile.std_dev and profile.mean and profile.mean != 0:
            cv = abs(profile.std_dev / profile.mean)
            if cv > 1.0:
                pattern_clarity = min(1.0, pattern_clarity + 0.1)

        reason = f"Median imputation for {null_pct:.1f}% null values"

        return ConfidenceResult.calculate(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            reversibility=reversibility,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )


class ModeImputationStrategy(BaseNullImputationStrategy):
    """Replace NULL values with the most frequent value (mode).

    Best for categorical columns with a dominant mode.
    """

    name = "mode_imputation"
    description = "Replace NULL values with the most frequent value (mode)"
    reversibility_score = 0.6

    def can_apply(self, context: FailureContext) -> bool:
        if context.test_type not in self.supported_test_types:
            return False
        return context.sample_data is not None and context.column_name is not None

    def _calculate_mode(self, context: FailureContext) -> tuple[Any, float]:
        """Calculate mode and its frequency from sample data."""
        values = context.get_sample_values()
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None, 0.0

        from collections import Counter

        counts = Counter(non_null)
        mode_value, mode_count = counts.most_common(1)[0]
        frequency = mode_count / len(non_null)
        return mode_value, frequency

    def _get_imputation_value(self, context: FailureContext) -> Any:
        mode_value, _ = self._calculate_mode(context)
        return mode_value

    def _get_value_description(self, value: Any) -> str:
        return f"mode '{value}'"

    def preview(self, context: FailureContext) -> PreviewResult:
        """Override to include frequency in summary."""
        mode_value, mode_freq = self._calculate_mode(context)
        column = context.column_name

        before_sample: list[dict[str, Any]] = []
        after_sample: list[dict[str, Any]] = []

        if context.sample_data and column:
            for row_dict in context.sample_data.to_dicts():
                if row_dict.get(column) is None:
                    before_sample.append(row_dict.copy())
                    after_row = row_dict.copy()
                    after_row[column] = mode_value
                    after_sample.append(after_row)
                    if len(before_sample) >= 5:
                        break

        affected = context.failed_rows or 0
        total = context.table_row_count

        return PreviewResult(
            before_sample=before_sample,
            after_sample=after_sample,
            changes_summary=f"Replace {affected} NULL values with mode '{mode_value}' ({mode_freq * 100:.1f}% frequency)",
            affected_rows=affected,
            total_rows=total,
        )

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        if not self.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")

        mode_value, mode_freq = self._calculate_mode(context)
        if mode_value is None:
            return ConfidenceResult(score=0.0, reason="No mode found")

        data_coverage = 0.8 if context.sample_data else 0.5
        pattern_clarity = min(1.0, mode_freq * 2)
        null_pct = context.null_percentage or 0
        impact_scope = max(0.0, 1.0 - (null_pct / 100)) if null_pct else 0.9
        reversibility = self.reversibility_score
        type_match = 0.9 if not context.is_numeric else 0.7

        reason = f"Mode '{mode_value}' appears in {mode_freq * 100:.1f}% of non-null values"
        if mode_freq < 0.3:
            reason += " (low dominance reduces confidence)"

        return ConfidenceResult.calculate(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            reversibility=reversibility,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )


class ForwardFillStrategy(FixStrategy):
    """Fill NULL values with the previous non-null value.

    Best for time-series data where values should persist until changed.
    Note: This strategy has different SQL generation logic and doesn't
    fit the BaseNullImputationStrategy pattern.
    """

    name = "forward_fill"
    description = "Fill NULL values with the previous non-null value (forward fill)"
    supported_test_types: ClassVar[list[str]] = ["columnValuesToNotBeNull"]
    reversibility_score = 0.7

    def __init__(self, order_column: str = "id"):
        self.order_column = order_column

    def can_apply(self, context: FailureContext) -> bool:
        if context.test_type not in self.supported_test_types:
            return False
        if not context.sample_data:
            return False
        if self.order_column not in context.sample_data.columns:
            if "id" in context.sample_data.columns:
                self.order_column = "id"
            elif "created_at" in context.sample_data.columns:
                self.order_column = "created_at"
            elif "timestamp" in context.sample_data.columns:
                self.order_column = "timestamp"
            else:
                return False
        return True

    def calculate_confidence(self, context: FailureContext) -> ConfidenceResult:
        if not self.can_apply(context):
            return ConfidenceResult(score=0.0, reason="Strategy not applicable")

        data_coverage = 0.7
        pattern_clarity = 0.7
        null_pct = context.null_percentage or 0
        impact_scope = max(0.0, 1.0 - (null_pct / 100)) if null_pct else 0.9
        reversibility = self.reversibility_score

        time_cols = {"created_at", "timestamp", "date", "datetime", "updated_at"}
        type_match = 0.9 if self.order_column.lower() in time_cols else 0.7

        reason = f"Forward fill using '{self.order_column}' ordering"

        return ConfidenceResult.calculate(
            data_coverage=data_coverage,
            pattern_clarity=pattern_clarity,
            reversibility=reversibility,
            impact_scope=impact_scope,
            type_match=type_match,
            reason=reason,
        )

    def generate_fix_sql(self, context: FailureContext) -> str:
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        order_col = self.order_column

        return f"""-- Forward fill NULL values using window function
WITH filled AS (
    SELECT *,
        COALESCE(
            {self._quote_identifier(column)},
            LAG({self._quote_identifier(column)}) IGNORE NULLS OVER (ORDER BY {self._quote_identifier(order_col)})
        ) AS filled_value
    FROM {table}
)
UPDATE {table} t
SET {self._quote_identifier(column)} = f.filled_value
FROM filled f
WHERE t.{self._quote_identifier(order_col)} = f.{self._quote_identifier(order_col)}
  AND t.{self._quote_identifier(column)} IS NULL;"""

    def generate_rollback_sql(self, context: FailureContext) -> str | None:
        table = self._get_full_table_ref(context)
        column = context.column_name
        assert column is not None
        table_name = self._get_table_name(context)

        return f"""-- Backup affected rows before applying fix:
CREATE TABLE {table_name}_backup_nulls AS
SELECT * FROM {table}
WHERE {self._quote_identifier(column)} IS NULL;"""

    def preview(self, context: FailureContext) -> PreviewResult:
        column = context.column_name

        before_sample: list[dict[str, Any]] = []
        after_sample: list[dict[str, Any]] = []

        if context.sample_data and column:
            last_value = None
            for row_dict in context.sample_data.to_dicts():
                current = row_dict.get(column)
                if current is not None:
                    last_value = current
                elif last_value is not None:
                    before_sample.append(row_dict.copy())
                    after_row = row_dict.copy()
                    after_row[column] = last_value
                    after_sample.append(after_row)
                    if len(before_sample) >= 5:
                        break

        affected = context.failed_rows or 0
        total = context.table_row_count

        return PreviewResult(
            before_sample=before_sample,
            after_sample=after_sample,
            changes_summary=f"Forward fill {affected} NULL values using previous non-null values",
            affected_rows=affected,
            total_rows=total,
        )
