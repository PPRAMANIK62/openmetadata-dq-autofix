"""Tests for deduplication strategies."""

from datetime import UTC, datetime

import pytest

from dq_autofix.openmetadata.models import (
    SampleData,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies import (
    FailureContext,
    KeepFirstStrategy,
    KeepLastStrategy,
)


class TestKeepFirstStrategy:
    """Tests for KeepFirstStrategy."""

    def test_can_apply_true(self, duplicate_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = KeepFirstStrategy()
        assert strategy.can_apply(duplicate_failure_context) is True

    def test_can_apply_false_wrong_test_type(self) -> None:
        """Test can_apply returns False for wrong test type."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        strategy = KeepFirstStrategy()
        assert strategy.can_apply(context) is False

    def test_can_apply_false_no_column(self) -> None:
        """Test can_apply returns False without column name."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table>",
        )
        context = FailureContext(test_case=test_case)
        strategy = KeepFirstStrategy()
        assert strategy.can_apply(context) is False

    def test_detect_order_column(self, duplicate_failure_context: FailureContext) -> None:
        """Test detection of ordering column."""
        strategy = KeepFirstStrategy()
        order_col = strategy._detect_order_column(duplicate_failure_context)
        assert order_col in ["id", "created_at"]

    def test_count_duplicates(self, duplicate_failure_context: FailureContext) -> None:
        """Test counting duplicate values."""
        strategy = KeepFirstStrategy()
        count, values = strategy._count_duplicates(duplicate_failure_context)
        assert count == 2
        assert "ORD-001" in values
        assert "ORD-002" in values

    def test_calculate_confidence(self, duplicate_failure_context: FailureContext) -> None:
        """Test confidence calculation for keep first."""
        strategy = KeepFirstStrategy()
        confidence = strategy.calculate_confidence(duplicate_failure_context)
        assert confidence.score > 0.5
        assert confidence.breakdown["reversibility"] == 0.0

    def test_generate_fix_sql(self, duplicate_failure_context: FailureContext) -> None:
        """Test SQL generation for keep first."""
        strategy = KeepFirstStrategy()
        sql = strategy.generate_fix_sql(duplicate_failure_context)
        assert "DELETE FROM" in sql
        assert "MIN" in sql
        assert "GROUP BY" in sql

    def test_generate_rollback_sql(self, duplicate_failure_context: FailureContext) -> None:
        """Test rollback SQL generation."""
        strategy = KeepFirstStrategy()
        rollback = strategy.generate_rollback_sql(duplicate_failure_context)
        assert rollback is not None
        assert "IMPORTANT" in rollback
        assert "backup" in rollback.lower()

    def test_preview(self, duplicate_failure_context: FailureContext) -> None:
        """Test preview generation for keep first."""
        strategy = KeepFirstStrategy()
        preview = strategy.preview(duplicate_failure_context)
        assert len(preview.before_sample) > 0
        assert len(preview.after_sample) == 0
        assert "Delete" in preview.changes_summary


class TestKeepLastStrategy:
    """Tests for KeepLastStrategy."""

    def test_can_apply_true(self, duplicate_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = KeepLastStrategy()
        assert strategy.can_apply(duplicate_failure_context) is True

    def test_detect_order_column_prefers_updated(self) -> None:
        """Test that updated_at is preferred for keep last."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::order_id>",
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "order_id", "created_at", "updated_at"],
            rows=[[1, "A", "2024-01-01", "2024-01-02"]],
        )
        context = FailureContext(test_case=test_case, sample_data=sample)
        strategy = KeepLastStrategy()
        order_col = strategy._detect_order_column(context)
        assert order_col == "updated_at"

    def test_calculate_confidence(self, duplicate_failure_context: FailureContext) -> None:
        """Test confidence calculation for keep last."""
        strategy = KeepLastStrategy()
        confidence = strategy.calculate_confidence(duplicate_failure_context)
        assert confidence.score > 0.5
        assert confidence.breakdown["reversibility"] == 0.0

    def test_generate_fix_sql(self, duplicate_failure_context: FailureContext) -> None:
        """Test SQL generation for keep last."""
        strategy = KeepLastStrategy()
        sql = strategy.generate_fix_sql(duplicate_failure_context)
        assert "DELETE FROM" in sql
        assert "MAX" in sql
        assert "GROUP BY" in sql

    def test_preview(self, duplicate_failure_context: FailureContext) -> None:
        """Test preview generation for keep last."""
        strategy = KeepLastStrategy()
        preview = strategy.preview(duplicate_failure_context)
        assert len(preview.before_sample) > 0
        assert "keeping last" in preview.changes_summary.lower()


class TestDeduplicationEdgeCases:
    """Edge case tests for deduplication strategies."""

    @pytest.fixture
    def no_duplicates_context(self) -> FailureContext:
        """Context with no actual duplicates in sample."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::order_id>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=0,
            ),
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "order_id"],
            rows=[[1, "A"], [2, "B"], [3, "C"]],
        )
        return FailureContext(test_case=test_case, sample_data=sample)

    def test_confidence_lower_without_duplicates(
        self, no_duplicates_context: FailureContext
    ) -> None:
        """Test confidence is lower when no duplicates in sample."""
        strategy = KeepFirstStrategy()
        confidence = strategy.calculate_confidence(no_duplicates_context)
        assert confidence.score < 0.8

    @pytest.fixture
    def many_duplicates_context(self) -> FailureContext:
        """Context with many duplicates."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::category>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=500,
                failed_rows_percentage=50.0,
            ),
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "category"],
            rows=[
                [1, "A"],
                [2, "A"],
                [3, "A"],
                [4, "B"],
                [5, "B"],
                [6, "C"],
            ],
        )
        return FailureContext(test_case=test_case, sample_data=sample, table_row_count=1000)

    def test_high_duplicate_percentage_affects_confidence(
        self, many_duplicates_context: FailureContext
    ) -> None:
        """Test that high duplicate % affects confidence."""
        strategy = KeepFirstStrategy()
        confidence = strategy.calculate_confidence(many_duplicates_context)
        assert confidence.breakdown["impact_scope"] < 0.5
