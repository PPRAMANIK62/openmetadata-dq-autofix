"""Tests for base strategy classes."""

from datetime import UTC, datetime

import pytest

from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies import ConfidenceResult, FailureContext, PreviewResult


class TestFailureContext:
    """Tests for FailureContext dataclass."""

    def test_table_fqn_extraction(self) -> None:
        """Test table FQN is extracted from test case."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        assert context.table_fqn == "db.schema.table"

    def test_column_name_extraction(self) -> None:
        """Test column name is extracted from test case."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::my_column>",
        )
        context = FailureContext(test_case=test_case)
        assert context.column_name == "my_column"

    def test_column_name_none_for_table_test(self) -> None:
        """Test column name is None for table-level tests."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="tableRowCount",
            entity_link="<#E::table::db.schema.table>",
        )
        context = FailureContext(test_case=test_case)
        assert context.column_name is None

    def test_test_type(self) -> None:
        """Test test type is accessible."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        assert context.test_type == "columnValuesToBeUnique"

    def test_failed_rows(self) -> None:
        """Test failed rows from result."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=100,
            ),
        )
        context = FailureContext(test_case=test_case)
        assert context.failed_rows == 100

    def test_is_numeric_true(self) -> None:
        """Test numeric type detection for integer."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="INTEGER")
        context = FailureContext(test_case=test_case, column_profile=profile)
        assert context.is_numeric is True

    def test_is_numeric_false(self) -> None:
        """Test numeric type detection for varchar."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="VARCHAR")
        context = FailureContext(test_case=test_case, column_profile=profile)
        assert context.is_numeric is False

    def test_get_sample_values(self) -> None:
        """Test extracting sample values for a column."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::value>",
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "value"],
            rows=[[1, "a"], [2, "b"], [3, None]],
        )
        context = FailureContext(test_case=test_case, sample_data=sample)
        values = context.get_sample_values()
        assert values == ["a", "b", None]

    def test_get_sample_values_no_sample_data(self) -> None:
        """Test get_sample_values returns empty list without sample data."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        assert context.get_sample_values() == []


class TestConfidenceResult:
    """Tests for ConfidenceResult dataclass."""

    def test_calculate_weighted_score(self) -> None:
        """Test confidence score calculation with weights."""
        result = ConfidenceResult.calculate(
            data_coverage=1.0,
            pattern_clarity=1.0,
            reversibility=1.0,
            impact_scope=1.0,
            type_match=1.0,
        )
        assert result.score == 1.0

    def test_calculate_zero_score(self) -> None:
        """Test confidence score calculation with zero values."""
        result = ConfidenceResult.calculate(
            data_coverage=0.0,
            pattern_clarity=0.0,
            reversibility=0.0,
            impact_scope=0.0,
            type_match=0.0,
        )
        assert result.score == 0.0

    def test_calculate_mixed_score(self) -> None:
        """Test confidence score calculation with mixed values."""
        result = ConfidenceResult.calculate(
            data_coverage=0.8,
            pattern_clarity=0.6,
            reversibility=0.5,
            impact_scope=0.9,
            type_match=1.0,
        )
        expected = 0.8 * 0.25 + 0.6 * 0.25 + 0.5 * 0.20 + 0.9 * 0.15 + 1.0 * 0.15
        assert result.score == pytest.approx(expected, rel=1e-3)

    def test_breakdown_included(self) -> None:
        """Test breakdown dict is populated."""
        result = ConfidenceResult.calculate(
            data_coverage=0.8,
            pattern_clarity=0.7,
            reversibility=0.6,
            impact_scope=0.9,
            type_match=0.95,
        )
        assert "data_coverage" in result.breakdown
        assert result.breakdown["data_coverage"] == 0.8
        assert result.breakdown["pattern_clarity"] == 0.7

    def test_is_high_threshold(self) -> None:
        """Test high confidence threshold."""
        high = ConfidenceResult(score=0.85)
        assert high.is_high is True
        assert high.is_medium is False

    def test_is_medium_threshold(self) -> None:
        """Test medium confidence threshold."""
        medium = ConfidenceResult(score=0.70)
        assert medium.is_medium is True
        assert medium.is_high is False
        assert medium.is_low is False

    def test_is_low_threshold(self) -> None:
        """Test low confidence threshold."""
        low = ConfidenceResult(score=0.45)
        assert low.is_low is True
        assert low.is_medium is False
        assert low.should_skip is False

    def test_should_skip(self) -> None:
        """Test skip threshold."""
        skip = ConfidenceResult(score=0.35)
        assert skip.should_skip is True


class TestPreviewResult:
    """Tests for PreviewResult dataclass."""

    def test_affected_percentage(self) -> None:
        """Test affected percentage calculation."""
        result = PreviewResult(
            before_sample=[{"a": 1}],
            after_sample=[{"a": 2}],
            changes_summary="test",
            affected_rows=100,
            total_rows=1000,
        )
        assert result.affected_percentage == 10.0

    def test_affected_percentage_none_without_total(self) -> None:
        """Test affected percentage is None without total rows."""
        result = PreviewResult(
            before_sample=[],
            after_sample=[],
            changes_summary="test",
            affected_rows=100,
            total_rows=None,
        )
        assert result.affected_percentage is None

    def test_affected_percentage_zero_total(self) -> None:
        """Test affected percentage with zero total rows."""
        result = PreviewResult(
            before_sample=[],
            after_sample=[],
            changes_summary="test",
            affected_rows=0,
            total_rows=0,
        )
        assert result.affected_percentage is None
