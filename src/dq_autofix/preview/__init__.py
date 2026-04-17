"""Preview and SQL generation utilities.

This module provides reusable utilities for generating:
- Before/after sample diffs
- SQL statements (UPDATE, DELETE)
- Backup and restore SQL for rollback
"""

from dq_autofix.preview.diff_generator import DiffGenerator, SampleDiff
from dq_autofix.preview.rollback import RollbackGenerator, RollbackSql
from dq_autofix.preview.sql_generator import SqlGenerator

__all__ = [
    "DiffGenerator",
    "RollbackGenerator",
    "RollbackSql",
    "SampleDiff",
    "SqlGenerator",
]
