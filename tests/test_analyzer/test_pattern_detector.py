"""Tests for PatternDetector."""

import pytest

from dq_autofix.analyzer.pattern_detector import (
    DetectedPattern,
    PatternDetector,
    PatternType,
)
from dq_autofix.openmetadata.models import (
    ColumnProfile,
    SampleData,
    TestCaseResult,
    TestCaseResultSummary,
    TestResultStatus,
)
from dq_autofix.strategies.base import FailureContext


class TestDetectedPattern:
    """Tests for DetectedPattern dataclass."""

    def test_is_significant_true(self) -> None:
        """Test is_significant returns True for significant patterns."""
        pattern = DetectedPattern(
            pattern_type=PatternType.WHITESPACE_ISSUES,
            confidence=0.8,
            affected_count=10,
        )
        assert pattern.is_significant is True

    def test_is_significant_false_low_confidence(self) -> None:
        """Test is_significant returns False for low confidence."""
        pattern = DetectedPattern(
            pattern_type=PatternType.WHITESPACE_ISSUES,
            confidence=0.3,
            affected_count=10,
        )
        assert pattern.is_significant is False

    def test_is_significant_false_zero_affected(self) -> None:
        """Test is_significant returns False when no rows affected."""
        pattern = DetectedPattern(
            pattern_type=PatternType.WHITESPACE_ISSUES,
            confidence=0.8,
            affected_count=0,
        )
        assert pattern.is_significant is False


class TestPatternDetector:
    """Tests for PatternDetector class."""

    @pytest.fixture
    def detector(self) -> PatternDetector:
        """Create a PatternDetector instance."""
        return PatternDetector()

    @pytest.fixture
    def null_test_case(self) -> TestCaseResult:
        """Create a null check test case."""
        from datetime import UTC, datetime

        return TestCaseResult(
            id="tc-null",
            name="test_nulls",
            test_definition="columnValuesToNotBeNull",
            entity_link="<#E::table::db.schema.table::columns::col>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=50,
                failed_rows_percentage=5.0,
            ),
        )

    @pytest.fixture
    def regex_test_case(self) -> TestCaseResult:
        """Create a regex match test case."""
        from datetime import UTC, datetime

        return TestCaseResult(
            id="tc-regex",
            name="test_email_format",
            test_definition="columnValuesToMatchRegex",
            entity_link="<#E::table::db.schema.table::columns::email>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=20,
                failed_rows_percentage=2.0,
            ),
        )

    @pytest.fixture
    def unique_test_case(self) -> TestCaseResult:
        """Create a uniqueness test case."""
        from datetime import UTC, datetime

        return TestCaseResult(
            id="tc-unique",
            name="test_unique_id",
            test_definition="columnValuesToBeUnique",
            entity_link="<#E::table::db.schema.table::columns::id>",
            result=TestCaseResultSummary(
                status=TestResultStatus.FAILED,
                timestamp=datetime.now(UTC),
                failed_rows=10,
                failed_rows_percentage=0.5,
            ),
        )

    def test_detect_null_patterns_sparse(
        self, detector: PatternDetector, null_test_case: TestCaseResult
    ) -> None:
        """Test detection of sparse null pattern (low null %)."""
        profile = ColumnProfile(name="col", null_proportion=0.02)
        context = FailureContext(test_case=null_test_case, column_profile=profile)

        patterns = detector.detect_patterns(context)

        assert len(patterns) >= 1
        null_patterns = [p for p in patterns if p.pattern_type == PatternType.SPARSE_NULLS]
        assert len(null_patterns) == 1
        assert null_patterns[0].confidence >= 0.8

    def test_detect_null_patterns_moderate(
        self, detector: PatternDetector, null_test_case: TestCaseResult
    ) -> None:
        """Test detection of null cluster pattern (moderate null %)."""
        profile = ColumnProfile(name="col", null_proportion=0.15)
        context = FailureContext(test_case=null_test_case, column_profile=profile)

        patterns = detector.detect_patterns(context)

        null_patterns = [p for p in patterns if p.pattern_type == PatternType.NULL_CLUSTER]
        assert len(null_patterns) == 1
        assert null_patterns[0].confidence >= 0.6

    def test_detect_null_patterns_high(
        self, detector: PatternDetector, null_test_case: TestCaseResult
    ) -> None:
        """Test detection of null cluster pattern (high null %)."""
        profile = ColumnProfile(name="col", null_proportion=0.35)
        context = FailureContext(test_case=null_test_case, column_profile=profile)

        patterns = detector.detect_patterns(context)

        null_patterns = [p for p in patterns if p.pattern_type == PatternType.NULL_CLUSTER]
        assert len(null_patterns) == 1
        assert null_patterns[0].confidence < 0.6

    def test_detect_whitespace_issues(
        self, detector: PatternDetector, regex_test_case: TestCaseResult
    ) -> None:
        """Test detection of whitespace issues."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["email"],
            rows=[
                ["  test@example.com"],
                ["valid@example.com"],
                ["spaces@test.com  "],
                ["  both@test.com  "],
            ],
        )
        context = FailureContext(test_case=regex_test_case, sample_data=sample)

        patterns = detector.detect_patterns(context)

        ws_patterns = [p for p in patterns if p.pattern_type == PatternType.WHITESPACE_ISSUES]
        assert len(ws_patterns) == 1
        assert ws_patterns[0].affected_count == 3
        assert ws_patterns[0].confidence >= 0.7

    def test_detect_case_inconsistency(
        self, detector: PatternDetector, regex_test_case: TestCaseResult
    ) -> None:
        """Test detection of case inconsistency."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["email"],
            rows=[
                ["test@example.com"],
                ["TEST@EXAMPLE.COM"],
                ["another@test.com"],
                ["Mixed@Test.Com"],
            ],
        )
        context = FailureContext(test_case=regex_test_case, sample_data=sample)

        patterns = detector.detect_patterns(context)

        case_patterns = [p for p in patterns if p.pattern_type == PatternType.CASE_INCONSISTENCY]
        assert len(case_patterns) == 1
        assert case_patterns[0].affected_count > 0

    def test_detect_format_mismatch_email(
        self, detector: PatternDetector, regex_test_case: TestCaseResult
    ) -> None:
        """Test detection of email format mismatch."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["email"],
            rows=[
                ["valid@example.com"],
                ["another@test.org"],
                ["invalid-email"],
                ["also-valid@domain.co"],
                ["not an email"],
            ],
        )
        context = FailureContext(test_case=regex_test_case, sample_data=sample)

        patterns = detector.detect_patterns(context)

        format_patterns = [p for p in patterns if p.pattern_type == PatternType.FORMAT_MISMATCH]
        assert len(format_patterns) == 1
        assert format_patterns[0].details.get("format_type") == "email"

    def test_detect_duplicate_patterns_low(
        self, detector: PatternDetector, unique_test_case: TestCaseResult
    ) -> None:
        """Test detection of duplicate patterns with low rate."""
        context = FailureContext(test_case=unique_test_case)

        patterns = detector.detect_patterns(context)

        dup_patterns = [p for p in patterns if p.pattern_type == PatternType.DUPLICATE_ROWS]
        assert len(dup_patterns) == 1
        assert dup_patterns[0].confidence >= 0.8

    def test_detect_numeric_patterns_outliers(
        self, detector: PatternDetector, null_test_case: TestCaseResult
    ) -> None:
        """Test detection of outlier pattern in numeric data."""
        profile = ColumnProfile(
            name="col",
            data_type="FLOAT",
            mean=100.0,
            median=50.0,
            std_dev=200.0,
        )
        context = FailureContext(test_case=null_test_case, column_profile=profile)

        patterns = detector._detect_numeric_patterns(context)

        outlier_patterns = [p for p in patterns if p.pattern_type == PatternType.OUTLIERS]
        assert len(outlier_patterns) == 1
        cv = outlier_patterns[0].details.get("coefficient_of_variation")
        assert cv is not None and cv > 1.0

    def test_detect_numeric_patterns_distribution(
        self, detector: PatternDetector, null_test_case: TestCaseResult
    ) -> None:
        """Test detection of distribution pattern in numeric data."""
        profile = ColumnProfile(
            name="col",
            data_type="INTEGER",
            mean=100.0,
            median=95.0,
            std_dev=10.0,
        )
        context = FailureContext(test_case=null_test_case, column_profile=profile)

        patterns = detector._detect_numeric_patterns(context)

        dist_patterns = [p for p in patterns if p.pattern_type == PatternType.NUMERIC_DISTRIBUTION]
        assert len(dist_patterns) == 1

    def test_detect_patterns_sorted_by_confidence(
        self, detector: PatternDetector, regex_test_case: TestCaseResult
    ) -> None:
        """Test that patterns are sorted by confidence descending."""
        sample = SampleData(
            table_fqn="db.schema.table",
            columns=["email"],
            rows=[
                ["  test@example.com"],
                ["  TEST@EXAMPLE.COM"],
                ["  valid@test.com"],
            ],
        )
        context = FailureContext(test_case=regex_test_case, sample_data=sample)

        patterns = detector.detect_patterns(context)

        for i in range(len(patterns) - 1):
            assert patterns[i].confidence >= patterns[i + 1].confidence

    def test_detect_pattern_clarity_single(self, detector: PatternDetector) -> None:
        """Test pattern clarity calculation with single pattern."""
        patterns = [
            DetectedPattern(
                pattern_type=PatternType.WHITESPACE_ISSUES,
                confidence=0.8,
                affected_count=10,
            )
        ]

        test_case = TestCaseResult(
            id="tc",
            name="test",
            test_definition="test",
            entity_link="<#E::table::t>",
        )
        context = FailureContext(test_case=test_case)

        clarity = detector.detect_pattern_clarity(context, patterns)
        assert clarity == 0.8

    def test_detect_pattern_clarity_multiple(self, detector: PatternDetector) -> None:
        """Test pattern clarity calculation with multiple patterns."""
        patterns = [
            DetectedPattern(PatternType.WHITESPACE_ISSUES, 0.9, 10),
            DetectedPattern(PatternType.CASE_INCONSISTENCY, 0.6, 5),
            DetectedPattern(PatternType.FORMAT_MISMATCH, 0.4, 3),
        ]
        test_case = TestCaseResult(
            id="tc",
            name="test",
            test_definition="test",
            entity_link="<#E::table::t>",
        )
        context = FailureContext(test_case=test_case)

        clarity = detector.detect_pattern_clarity(context, patterns)
        assert 0.5 <= clarity <= 1.0

    def test_detect_pattern_clarity_empty(self, detector: PatternDetector) -> None:
        """Test pattern clarity calculation with no patterns."""
        test_case = TestCaseResult(
            id="tc",
            name="test",
            test_definition="test",
            entity_link="<#E::table::t>",
        )
        context = FailureContext(test_case=test_case)

        clarity = detector.detect_pattern_clarity(context, [])
        assert clarity == 0.5

    def test_detect_patterns_no_sample_data(
        self, detector: PatternDetector, regex_test_case: TestCaseResult
    ) -> None:
        """Test detection with no sample data returns empty for string patterns."""
        context = FailureContext(test_case=regex_test_case)

        patterns = detector._detect_string_patterns(context)
        assert patterns == []
