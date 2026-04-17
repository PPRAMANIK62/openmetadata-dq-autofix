"""Tests for DiffGenerator utility."""

from dq_autofix.preview import DiffGenerator, SampleDiff
from dq_autofix.strategies.base import FailureContext, PreviewResult


class TestBuildSampleDiff:
    """Tests for DiffGenerator.build_sample_diff()."""

    def test_filters_correctly(self, simple_context: FailureContext) -> None:
        """Test that build_sample_diff applies filter correctly."""
        diff = DiffGenerator.build_sample_diff(
            simple_context,
            column="value",
            should_include=lambda v: v is None,
            transform=lambda _: 999,
        )

        assert len(diff.before) == 2
        assert len(diff.after) == 2
        assert all(row["value"] is None for row in diff.before)
        assert all(row["value"] == 999 for row in diff.after)

    def test_transforms_values(self, simple_context: FailureContext) -> None:
        """Test that transform function is applied."""
        diff = DiffGenerator.build_sample_diff(
            simple_context,
            column="value",
            should_include=lambda v: v is not None,
            transform=lambda v: v * 2,
        )

        for before_row, after_row in zip(diff.before, diff.after, strict=True):
            assert after_row["value"] == before_row["value"] * 2

    def test_respects_max_samples(self, simple_context: FailureContext) -> None:
        """Test max_samples limit."""
        diff = DiffGenerator.build_sample_diff(
            simple_context,
            column="value",
            should_include=lambda _: True,
            transform=lambda v: v,
            max_samples=2,
        )

        assert len(diff.before) == 2
        assert len(diff.after) == 2

    def test_empty_when_no_matches(self, simple_context: FailureContext) -> None:
        """Test empty result when no rows match filter."""
        diff = DiffGenerator.build_sample_diff(
            simple_context,
            column="value",
            should_include=lambda v: v == -999,
            transform=lambda v: v,
        )

        assert len(diff.before) == 0
        assert len(diff.after) == 0

    def test_handles_missing_sample_data(self, simple_test_case) -> None:
        """Test handling when sample_data is None."""
        context = FailureContext(test_case=simple_test_case, sample_data=None)

        diff = DiffGenerator.build_sample_diff(
            context,
            column="value",
            should_include=lambda _: True,
            transform=lambda v: v,
        )

        assert len(diff.before) == 0
        assert len(diff.after) == 0


class TestBuildPreviewResult:
    """Tests for DiffGenerator.build_preview_result()."""

    def test_builds_preview_result(self) -> None:
        """Test building PreviewResult from SampleDiff."""
        diff = SampleDiff(
            before=[{"id": 1, "value": None}],
            after=[{"id": 1, "value": 100}],
        )

        result = DiffGenerator.build_preview_result(
            diff,
            changes_summary="Test summary",
            affected_rows=10,
            total_rows=100,
        )

        assert isinstance(result, PreviewResult)
        assert result.before_sample == diff.before
        assert result.after_sample == diff.after
        assert result.changes_summary == "Test summary"
        assert result.affected_rows == 10
        assert result.total_rows == 100

    def test_calculates_affected_percentage(self) -> None:
        """Test affected_percentage calculation."""
        diff = SampleDiff(before=[], after=[])
        result = DiffGenerator.build_preview_result(
            diff,
            changes_summary="Test",
            affected_rows=25,
            total_rows=100,
        )

        assert result.affected_percentage == 25.0


class TestFormatUnifiedDiff:
    """Tests for DiffGenerator.format_unified_diff()."""

    def test_format_unified_diff(self) -> None:
        """Test unified diff formatting."""
        before = [{"id": 1, "value": None}]
        after = [{"id": 1, "value": 100}]

        diff_text = DiffGenerator.format_unified_diff(before, after)

        assert "---" in diff_text
        assert "+++" in diff_text
        assert "before" in diff_text
        assert "after" in diff_text

    def test_empty_diff_when_identical(self) -> None:
        """Test empty diff when before and after are identical."""
        data = [{"id": 1, "value": 100}]

        diff_text = DiffGenerator.format_unified_diff(data, data)

        assert diff_text == ""


class TestFormatSideBySide:
    """Tests for DiffGenerator.format_side_by_side()."""

    def test_format_side_by_side(self) -> None:
        """Test side-by-side formatting."""
        before = [{"id": 1, "value": None}]
        after = [{"id": 1, "value": 100}]

        result = DiffGenerator.format_side_by_side(before, after)

        assert "BEFORE" in result
        assert "AFTER" in result
        assert "|" in result

    def test_handles_unequal_lengths(self) -> None:
        """Test handling when before and after have different lengths."""
        before = [{"id": 1}, {"id": 2}]
        after = [{"id": 1}]

        result = DiffGenerator.format_side_by_side(before, after)

        lines = result.split("\n")
        assert len(lines) >= 3
