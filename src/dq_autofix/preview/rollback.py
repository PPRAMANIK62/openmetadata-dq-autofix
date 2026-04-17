"""Utilities for generating rollback SQL."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from dq_autofix.preview.sql_generator import SqlGenerator


@dataclass
class RollbackSql:
    """Backup and restore SQL statements."""

    backup_sql: str
    restore_sql: str | None = None
    warnings: list[str] = field(default_factory=list)


class RollbackGenerator:
    """Utilities for generating rollback SQL."""

    @staticmethod
    def build_backup_sql(
        table: str,
        table_name: str,
        suffix: str,
        where_clause: str,
        comment: str = "",
        include_timestamp: bool = True,
    ) -> str:
        """Generate CREATE TABLE AS SELECT for backup.

        Args:
            table: Full table reference (schema.table).
            table_name: Simple table name for backup naming.
            suffix: Suffix for backup table (e.g., 'nulls', 'duplicates').
            where_clause: WHERE condition to filter rows.
            comment: Optional comment to prepend.
            include_timestamp: Whether to add timestamp to backup name.

        Returns:
            CREATE TABLE AS SELECT statement.
        """
        if include_timestamp:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_table = f"{table_name}_backup_{suffix}_{timestamp}"
        else:
            backup_table = f"{table_name}_backup_{suffix}"

        comment_line = f"-- {comment}\n" if comment else ""

        return f"""{comment_line}CREATE TABLE {backup_table} AS
SELECT * FROM {table}
WHERE {where_clause};"""

    @staticmethod
    def build_restore_sql(
        backup_table: str,
        target_table: str,
        join_column: str,
        restore_column: str,
    ) -> str:
        """Generate SQL to restore column from backup.

        Args:
            backup_table: Name of backup table.
            target_table: Full target table reference.
            join_column: Column to join on (e.g., 'id').
            restore_column: Column to restore values for.

        Returns:
            UPDATE FROM statement for restoration.
        """
        quoted_join = SqlGenerator.quote_identifier(join_column)
        quoted_col = SqlGenerator.quote_identifier(restore_column)

        return f"""-- Restore from backup:
UPDATE {target_table} t
SET {quoted_col} = b.{quoted_col}
FROM {backup_table} b
WHERE t.{quoted_join} = b.{quoted_join};"""

    @staticmethod
    def build_full_backup_sql(
        table: str,
        table_name: str,
        include_timestamp: bool = True,
    ) -> str:
        """Generate SQL to backup entire table.

        Args:
            table: Full table reference (schema.table).
            table_name: Simple table name for backup naming.
            include_timestamp: Whether to add timestamp to backup name.

        Returns:
            CREATE TABLE AS SELECT statement for full backup.
        """
        if include_timestamp:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_table = f"{table_name}_backup_full_{timestamp}"
        else:
            backup_table = f"{table_name}_backup_full"

        return f"""-- IMPORTANT: Full table backup before destructive operation
CREATE TABLE {backup_table} AS
SELECT * FROM {table};"""

    @staticmethod
    def build_full_rollback(
        table: str,
        table_name: str,
        suffix: str,
        where_clause: str,
        join_column: str,
        restore_column: str,
        reversibility_score: float,
    ) -> RollbackSql:
        """Generate complete rollback with backup and restore.

        Args:
            table: Full table reference.
            table_name: Simple table name.
            suffix: Backup suffix.
            where_clause: Backup filter condition.
            join_column: Column for restore join.
            restore_column: Column to restore.
            reversibility_score: Strategy's reversibility score.

        Returns:
            RollbackSql with backup, restore, and warnings.
        """
        warnings: list[str] = []

        if reversibility_score < 0.5:
            warnings.append("Low reversibility: data may be lost permanently")
        if reversibility_score == 0.0:
            warnings.append("DESTRUCTIVE: This operation cannot be undone")

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_table = f"{table_name}_backup_{suffix}_{timestamp}"

        backup_sql = RollbackGenerator.build_backup_sql(
            table,
            table_name,
            suffix,
            where_clause,
            comment="Backup affected rows before applying fix",
            include_timestamp=False,
        )
        backup_sql = backup_sql.replace(f"{table_name}_backup_{suffix}", backup_table)

        restore_sql = None
        if reversibility_score > 0:
            restore_sql = RollbackGenerator.build_restore_sql(
                backup_table, table, join_column, restore_column
            )

        return RollbackSql(
            backup_sql=backup_sql,
            restore_sql=restore_sql,
            warnings=warnings,
        )

    @staticmethod
    def get_reversibility_warning(score: float) -> str | None:
        """Get warning message based on reversibility score.

        Args:
            score: Reversibility score (0.0 to 1.0).

        Returns:
            Warning message or None if highly reversible.
        """
        if score == 0.0:
            return "DESTRUCTIVE: This operation cannot be undone without a backup"
        if score < 0.3:
            return "Very low reversibility: Most changes cannot be undone"
        if score < 0.5:
            return "Low reversibility: Some data may be lost permanently"
        if score < 0.7:
            return "Moderate reversibility: Partial restoration possible"
        return None
