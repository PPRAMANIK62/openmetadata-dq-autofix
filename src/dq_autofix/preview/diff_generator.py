"""Utilities for generating preview diffs."""

import difflib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dq_autofix.strategies.base import FailureContext, PreviewResult


@dataclass
class SampleDiff:
    """Before/after sample data."""

    before: list[dict[str, Any]]
    after: list[dict[str, Any]]


class DiffGenerator:
    """Utilities for generating preview diffs."""

    @staticmethod
    def build_sample_diff(
        context: FailureContext,
        column: str,
        should_include: Callable[[Any], bool],
        transform: Callable[[Any], Any],
        max_samples: int = 5,
    ) -> SampleDiff:
        """Build before/after samples using callbacks.

        Args:
            context: Failure context with sample data.
            column: Column being transformed.
            should_include: Predicate to test if row should be included.
            transform: Function to transform the column value.
            max_samples: Maximum samples to include.

        Returns:
            SampleDiff with before and after lists.
        """
        before: list[dict[str, Any]] = []
        after: list[dict[str, Any]] = []

        if context.sample_data and column:
            for row_dict in context.sample_data.to_dicts():
                value = row_dict.get(column)
                if should_include(value):
                    before.append(row_dict.copy())
                    after_row = row_dict.copy()
                    after_row[column] = transform(value)
                    after.append(after_row)
                    if len(before) >= max_samples:
                        break

        return SampleDiff(before=before, after=after)

    @staticmethod
    def build_preview_result(
        diff: SampleDiff,
        changes_summary: str,
        affected_rows: int,
        total_rows: int | None = None,
    ) -> PreviewResult:
        """Build PreviewResult from SampleDiff.

        Args:
            diff: SampleDiff with before/after data.
            changes_summary: Human-readable summary of changes.
            affected_rows: Number of rows affected.
            total_rows: Total rows in table (optional).

        Returns:
            PreviewResult instance.
        """
        return PreviewResult(
            before_sample=diff.before,
            after_sample=diff.after,
            changes_summary=changes_summary,
            affected_rows=affected_rows,
            total_rows=total_rows,
        )

    @staticmethod
    def format_unified_diff(
        before: list[dict[str, Any]],
        after: list[dict[str, Any]],
        context_lines: int = 3,
    ) -> str:
        """Format samples as unified diff text.

        Args:
            before: Before sample rows.
            after: After sample rows.
            context_lines: Lines of context around changes.

        Returns:
            Unified diff string.
        """
        before_lines = [json.dumps(row, indent=2, default=str) for row in before]
        after_lines = [json.dumps(row, indent=2, default=str) for row in after]

        diff = difflib.unified_diff(
            before_lines,
            after_lines,
            fromfile="before",
            tofile="after",
            lineterm="",
            n=context_lines,
        )
        return "\n".join(diff)

    @staticmethod
    def format_side_by_side(
        before: list[dict[str, Any]],
        after: list[dict[str, Any]],
        column_width: int = 40,
    ) -> str:
        """Format samples as side-by-side comparison.

        Args:
            before: Before sample rows.
            after: After sample rows.
            column_width: Width of each column.

        Returns:
            Side-by-side formatted string.
        """
        lines = []
        header = f"{'BEFORE':<{column_width}} | {'AFTER':<{column_width}}"
        lines.append(header)
        lines.append("-" * len(header))

        max_rows = max(len(before), len(after))
        for i in range(max_rows):
            before_str = json.dumps(before[i], default=str) if i < len(before) else ""
            after_str = json.dumps(after[i], default=str) if i < len(after) else ""

            before_str = before_str[:column_width] if len(before_str) > column_width else before_str
            after_str = after_str[:column_width] if len(after_str) > column_width else after_str

            lines.append(f"{before_str:<{column_width}} | {after_str:<{column_width}}")

        return "\n".join(lines)
