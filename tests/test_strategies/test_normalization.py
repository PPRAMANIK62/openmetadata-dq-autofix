"""Tests for normalization strategies."""

from datetime import UTC, datetime

import pytest

from dq_autofix.openmetadata.models import (
    SampleData,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies import (
    CaseType,
    FailureContext,
    NormalizeCaseStrategy,
    TrimWhitespaceStrategy,
)


class TestTrimWhitespaceStrategy:
    """Tests for TrimWhitespaceStrategy."""

    def test_can_apply_true(self, whitespace_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = TrimWhitespaceStrategy()
        assert strategy.can_apply(whitespace_failure_context) is True

    def test_can_apply_false_wrong_test_type(self) -> None:
        """Test can_apply returns False for wrong test type."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        strategy = TrimWhitespaceStrategy()
        assert strategy.can_apply(context) is False

    def test_can_apply_false_no_column(self) -> None:
        """Test can_apply returns False without column name."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToMatchRegex",
            entity_link="<#E::table::db.schema.table>",
        )
        context = FailureContext(test_case=test_case)
        strategy = TrimWhitespaceStrategy()
        assert strategy.can_apply(context) is False

    def test_count_issues(self, whitespace_failure_context: FailureContext) -> None:
        """Test counting values with whitespace issues."""
        strategy = TrimWhitespaceStrategy()
        count = strategy._count_issues(whitespace_failure_context)
        assert count == 3

    def test_calculate_confidence(self, whitespace_failure_context: FailureContext) -> None:
        """Test confidence calculation for trim whitespace."""
        strategy = TrimWhitespaceStrategy()
        confidence = strategy.calculate_confidence(whitespace_failure_context)
        assert confidence.score > 0.8
        assert confidence.breakdown["reversibility"] == 1.0

    def test_generate_fix_sql(self, whitespace_failure_context: FailureContext) -> None:
        """Test SQL generation for trim whitespace."""
        strategy = TrimWhitespaceStrategy()
        sql = strategy.generate_fix_sql(whitespace_failure_context)
        assert "UPDATE" in sql
        assert "TRIM" in sql

    def test_generate_rollback_sql(self, whitespace_failure_context: FailureContext) -> None:
        """Test rollback SQL generation."""
        strategy = TrimWhitespaceStrategy()
        rollback = strategy.generate_rollback_sql(whitespace_failure_context)
        assert rollback is not None
        assert "backup" in rollback.lower()

    def test_preview(self, whitespace_failure_context: FailureContext) -> None:
        """Test preview generation for trim."""
        strategy = TrimWhitespaceStrategy()
        preview = strategy.preview(whitespace_failure_context)
        assert len(preview.before_sample) > 0
        assert len(preview.after_sample) > 0

        for before, after in zip(preview.before_sample, preview.after_sample, strict=False):
            before_email = before.get("email", "")
            after_email = after.get("email", "")
            if isinstance(before_email, str) and isinstance(after_email, str):
                assert after_email == before_email.strip()


class TestNormalizeCaseStrategy:
    """Tests for NormalizeCaseStrategy."""

    @pytest.fixture
    def case_failure_context(self) -> FailureContext:
        """Failure context with mixed case values."""
        test_case = TestCaseResult(
            id="tc-001",
            name="status_check",
            test_definition="columnValuesToBeInSet",
            entity_link="<#E::table::db.schema.table::columns::status>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=50,
            ),
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["id", "status"],
            rows=[
                [1, "active"],
                [2, "ACTIVE"],
                [3, "Active"],
                [4, "pending"],
                [5, "PENDING"],
            ],
        )
        return FailureContext(test_case=test_case, sample_data=sample)

    def test_can_apply_true(self, case_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = NormalizeCaseStrategy(CaseType.LOWER)
        assert strategy.can_apply(case_failure_context) is True

    def test_can_apply_false_wrong_test_type(self) -> None:
        """Test can_apply returns False for wrong test type."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        strategy = NormalizeCaseStrategy()
        assert strategy.can_apply(context) is False

    def test_detect_case_issues(self, case_failure_context: FailureContext) -> None:
        """Test detection of case inconsistencies."""
        strategy = NormalizeCaseStrategy()
        mixed_count, predominant = strategy._detect_case_issues(case_failure_context)
        assert mixed_count > 0
        assert predominant == "mixed"

    def test_calculate_confidence(self, case_failure_context: FailureContext) -> None:
        """Test confidence calculation for case normalization."""
        strategy = NormalizeCaseStrategy(CaseType.LOWER)
        confidence = strategy.calculate_confidence(case_failure_context)
        assert confidence.score > 0.6

    def test_generate_fix_sql_lower(self, case_failure_context: FailureContext) -> None:
        """Test SQL generation for lowercase normalization."""
        strategy = NormalizeCaseStrategy(CaseType.LOWER)
        sql = strategy.generate_fix_sql(case_failure_context)
        assert "UPDATE" in sql
        assert "LOWER" in sql

    def test_generate_fix_sql_upper(self, case_failure_context: FailureContext) -> None:
        """Test SQL generation for uppercase normalization."""
        strategy = NormalizeCaseStrategy(CaseType.UPPER)
        sql = strategy.generate_fix_sql(case_failure_context)
        assert "UPPER" in sql

    def test_generate_fix_sql_title(self, case_failure_context: FailureContext) -> None:
        """Test SQL generation for title case normalization."""
        strategy = NormalizeCaseStrategy(CaseType.TITLE)
        sql = strategy.generate_fix_sql(case_failure_context)
        assert "INITCAP" in sql

    def test_generate_rollback_sql(self, case_failure_context: FailureContext) -> None:
        """Test rollback SQL generation."""
        strategy = NormalizeCaseStrategy()
        rollback = strategy.generate_rollback_sql(case_failure_context)
        assert rollback is not None
        assert "backup" in rollback.lower()

    def test_preview_lower(self, case_failure_context: FailureContext) -> None:
        """Test preview generation for lowercase."""
        strategy = NormalizeCaseStrategy(CaseType.LOWER)
        preview = strategy.preview(case_failure_context)
        assert len(preview.before_sample) > 0

        for after in preview.after_sample:
            status = after.get("status")
            if status:
                assert status == status.lower()

    def test_preview_upper(self, case_failure_context: FailureContext) -> None:
        """Test preview generation for uppercase."""
        strategy = NormalizeCaseStrategy(CaseType.UPPER)
        preview = strategy.preview(case_failure_context)

        for after in preview.after_sample:
            status = after.get("status")
            if status:
                assert status == status.upper()


class TestNormalizationWithBeInSet:
    """Test normalization strategies with columnValuesToBeInSet."""

    @pytest.fixture
    def set_failure_context(self) -> FailureContext:
        """Context for set membership test."""
        test_case = TestCaseResult(
            id="tc-001",
            name="valid_categories",
            test_definition="columnValuesToBeInSet",
            entity_link="<#E::table::db.schema.products::columns::category>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=100,
            ),
        )
        sample = SampleData(
            table_fqn="db.schema.products",
            columns=["id", "category"],
            rows=[
                [1, "Electronics"],
                [2, "ELECTRONICS"],
                [3, "  Electronics  "],
                [4, "Clothing"],
            ],
        )
        return FailureContext(test_case=test_case, sample_data=sample)

    def test_trim_can_apply(self, set_failure_context: FailureContext) -> None:
        """Test trim can apply to set membership failures."""
        strategy = TrimWhitespaceStrategy()
        assert strategy.can_apply(set_failure_context) is True

    def test_case_can_apply(self, set_failure_context: FailureContext) -> None:
        """Test case normalization can apply to set membership failures."""
        strategy = NormalizeCaseStrategy()
        assert strategy.can_apply(set_failure_context) is True
