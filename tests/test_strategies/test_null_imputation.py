"""Tests for null imputation strategies."""

from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TestCaseResult,
)
from dq_autofix.strategies import (
    FailureContext,
    ForwardFillStrategy,
    MeanImputationStrategy,
    MedianImputationStrategy,
    ModeImputationStrategy,
)


class TestMeanImputationStrategy:
    """Tests for MeanImputationStrategy."""

    def test_can_apply_true(self, null_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = MeanImputationStrategy()
        assert strategy.can_apply(null_failure_context) is True

    def test_can_apply_false_wrong_test_type(self) -> None:
        """Test can_apply returns False for wrong test type."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="INTEGER", mean=100)
        context = FailureContext(test_case=test_case, column_profile=profile)
        strategy = MeanImputationStrategy()
        assert strategy.can_apply(context) is False

    def test_can_apply_false_non_numeric(self) -> None:
        """Test can_apply returns False for non-numeric column."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="VARCHAR", mean=None)
        context = FailureContext(test_case=test_case, column_profile=profile)
        strategy = MeanImputationStrategy()
        assert strategy.can_apply(context) is False

    def test_can_apply_false_no_mean(self) -> None:
        """Test can_apply returns False when no mean available."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="INTEGER", mean=None)
        context = FailureContext(test_case=test_case, column_profile=profile)
        strategy = MeanImputationStrategy()
        assert strategy.can_apply(context) is False

    def test_calculate_confidence(self, null_failure_context: FailureContext) -> None:
        """Test confidence calculation for mean imputation."""
        strategy = MeanImputationStrategy()
        confidence = strategy.calculate_confidence(null_failure_context)
        assert confidence.score > 0.6
        assert "data_coverage" in confidence.breakdown

    def test_generate_fix_sql(self, null_failure_context: FailureContext) -> None:
        """Test SQL generation for mean imputation."""
        strategy = MeanImputationStrategy()
        sql = strategy.generate_fix_sql(null_failure_context)
        assert "UPDATE" in sql
        assert "SET" in sql
        assert "IS NULL" in sql
        assert "45892.5" in sql

    def test_generate_rollback_sql(self, null_failure_context: FailureContext) -> None:
        """Test rollback SQL generation."""
        strategy = MeanImputationStrategy()
        rollback = strategy.generate_rollback_sql(null_failure_context)
        assert rollback is not None
        assert "backup" in rollback.lower()

    def test_preview(self, null_failure_context: FailureContext) -> None:
        """Test preview generation."""
        strategy = MeanImputationStrategy()
        preview = strategy.preview(null_failure_context)
        assert len(preview.before_sample) > 0
        assert len(preview.after_sample) > 0
        assert preview.affected_rows == 127


class TestMedianImputationStrategy:
    """Tests for MedianImputationStrategy."""

    def test_can_apply_true(self, null_failure_context: FailureContext) -> None:
        """Test can_apply returns True for valid context."""
        strategy = MedianImputationStrategy()
        assert strategy.can_apply(null_failure_context) is True

    def test_can_apply_false_no_median(self) -> None:
        """Test can_apply returns False when no median available."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        profile = ColumnProfile(name="col", data_type="INTEGER", median=None)
        context = FailureContext(test_case=test_case, column_profile=profile)
        strategy = MedianImputationStrategy()
        assert strategy.can_apply(context) is False

    def test_calculate_confidence(self, null_failure_context: FailureContext) -> None:
        """Test confidence calculation for median imputation."""
        strategy = MedianImputationStrategy()
        confidence = strategy.calculate_confidence(null_failure_context)
        assert confidence.score > 0.6

    def test_generate_fix_sql(self, null_failure_context: FailureContext) -> None:
        """Test SQL generation for median imputation."""
        strategy = MedianImputationStrategy()
        sql = strategy.generate_fix_sql(null_failure_context)
        assert "UPDATE" in sql
        assert "45000.0" in sql

    def test_preview(self, null_failure_context: FailureContext) -> None:
        """Test preview generation."""
        strategy = MedianImputationStrategy()
        preview = strategy.preview(null_failure_context)
        assert preview.changes_summary.startswith("Replace")


class TestModeImputationStrategy:
    """Tests for ModeImputationStrategy."""

    def test_can_apply_true(self, categorical_failure_context: FailureContext) -> None:
        """Test can_apply returns True for categorical context."""
        strategy = ModeImputationStrategy()
        assert strategy.can_apply(categorical_failure_context) is True

    def test_can_apply_false_no_sample_data(self) -> None:
        """Test can_apply returns False without sample data."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::status>",
        )
        context = FailureContext(test_case=test_case)
        strategy = ModeImputationStrategy()
        assert strategy.can_apply(context) is False

    def test_calculate_mode(self, categorical_failure_context: FailureContext) -> None:
        """Test mode calculation from sample data."""
        strategy = ModeImputationStrategy()
        mode_value, freq = strategy._calculate_mode(categorical_failure_context)
        assert mode_value == "pending"
        assert freq > 0.5

    def test_calculate_confidence(self, categorical_failure_context: FailureContext) -> None:
        """Test confidence calculation for mode imputation."""
        strategy = ModeImputationStrategy()
        confidence = strategy.calculate_confidence(categorical_failure_context)
        assert confidence.score > 0.5

    def test_generate_fix_sql(self, categorical_failure_context: FailureContext) -> None:
        """Test SQL generation for mode imputation."""
        strategy = ModeImputationStrategy()
        sql = strategy.generate_fix_sql(categorical_failure_context)
        assert "UPDATE" in sql
        assert "'pending'" in sql

    def test_preview(self, categorical_failure_context: FailureContext) -> None:
        """Test preview generation."""
        strategy = ModeImputationStrategy()
        preview = strategy.preview(categorical_failure_context)
        assert "mode" in preview.changes_summary.lower()


class TestForwardFillStrategy:
    """Tests for ForwardFillStrategy."""

    def test_can_apply_true_with_id(self, null_failure_context: FailureContext) -> None:
        """Test can_apply returns True when id column exists."""
        strategy = ForwardFillStrategy()
        assert strategy.can_apply(null_failure_context) is True

    def test_can_apply_false_no_sample_data(self) -> None:
        """Test can_apply returns False without sample data."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)
        strategy = ForwardFillStrategy()
        assert strategy.can_apply(context) is False

    def test_can_apply_with_timestamp_column(self) -> None:
        """Test can_apply with timestamp ordering column."""
        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::value>",
        )
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["timestamp", "value"],
            rows=[["2024-01-01", 1], ["2024-01-02", None]],
        )
        context = FailureContext(test_case=test_case, sample_data=sample)
        strategy = ForwardFillStrategy()
        assert strategy.can_apply(context) is True
        assert strategy.order_column == "timestamp"

    def test_calculate_confidence(self, null_failure_context: FailureContext) -> None:
        """Test confidence calculation for forward fill."""
        strategy = ForwardFillStrategy()
        confidence = strategy.calculate_confidence(null_failure_context)
        assert confidence.score > 0.5

    def test_generate_fix_sql(self, null_failure_context: FailureContext) -> None:
        """Test SQL generation for forward fill."""
        strategy = ForwardFillStrategy()
        sql = strategy.generate_fix_sql(null_failure_context)
        assert "LAG" in sql or "COALESCE" in sql
        assert "OVER" in sql

    def test_preview(self, null_failure_context: FailureContext) -> None:
        """Test preview generation."""
        strategy = ForwardFillStrategy()
        preview = strategy.preview(null_failure_context)
        assert "forward fill" in preview.changes_summary.lower()
