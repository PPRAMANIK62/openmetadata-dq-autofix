"""Tests for RollbackGenerator utility."""

from dq_autofix.preview import RollbackGenerator


class TestBuildBackupSql:
    """Tests for RollbackGenerator.build_backup_sql()."""

    def test_build_backup_sql(self) -> None:
        """Test backup table SQL generation."""
        sql = RollbackGenerator.build_backup_sql(
            table="schema.table",
            table_name="table",
            suffix="nulls",
            where_clause='"value" IS NULL',
            include_timestamp=False,
        )

        assert "CREATE TABLE table_backup_nulls AS" in sql
        assert "SELECT * FROM schema.table" in sql
        assert 'WHERE "value" IS NULL' in sql

    def test_backup_sql_with_comment(self) -> None:
        """Test backup SQL with comment."""
        sql = RollbackGenerator.build_backup_sql(
            table="schema.table",
            table_name="table",
            suffix="test",
            where_clause="1=1",
            comment="Test backup",
            include_timestamp=False,
        )

        assert "-- Test backup" in sql

    def test_backup_sql_with_timestamp(self) -> None:
        """Test backup SQL includes timestamp when enabled."""
        sql = RollbackGenerator.build_backup_sql(
            table="schema.table",
            table_name="table",
            suffix="test",
            where_clause="1=1",
            include_timestamp=True,
        )

        assert "table_backup_test_" in sql
        assert len(sql.split("table_backup_test_")[1].split()[0]) > 5


class TestBuildRestoreSql:
    """Tests for RollbackGenerator.build_restore_sql()."""

    def test_build_restore_sql(self) -> None:
        """Test restore SQL generation."""
        sql = RollbackGenerator.build_restore_sql(
            backup_table="table_backup_nulls",
            target_table="schema.table",
            join_column="id",
            restore_column="value",
        )

        assert "UPDATE schema.table t" in sql
        assert 'SET "value" = b."value"' in sql
        assert "FROM table_backup_nulls b" in sql
        assert 't."id" = b."id"' in sql

    def test_restore_sql_has_comment(self) -> None:
        """Test restore SQL has explanatory comment."""
        sql = RollbackGenerator.build_restore_sql(
            backup_table="backup",
            target_table="table",
            join_column="id",
            restore_column="col",
        )

        assert "Restore from backup" in sql


class TestBuildFullBackupSql:
    """Tests for RollbackGenerator.build_full_backup_sql()."""

    def test_build_full_backup_sql(self) -> None:
        """Test full table backup SQL generation."""
        sql = RollbackGenerator.build_full_backup_sql(
            table="schema.table",
            table_name="table",
            include_timestamp=False,
        )

        assert "CREATE TABLE table_backup_full AS" in sql
        assert "SELECT * FROM schema.table" in sql
        assert "IMPORTANT" in sql


class TestBuildFullRollback:
    """Tests for RollbackGenerator.build_full_rollback()."""

    def test_build_full_rollback(self) -> None:
        """Test complete rollback generation."""
        result = RollbackGenerator.build_full_rollback(
            table="schema.table",
            table_name="table",
            suffix="nulls",
            where_clause='"value" IS NULL',
            join_column="id",
            restore_column="value",
            reversibility_score=0.5,
        )

        assert result.backup_sql is not None
        assert result.restore_sql is not None
        assert "CREATE TABLE" in result.backup_sql
        assert "UPDATE" in result.restore_sql

    def test_no_restore_for_zero_reversibility(self) -> None:
        """Test no restore SQL for irreversible operations."""
        result = RollbackGenerator.build_full_rollback(
            table="schema.table",
            table_name="table",
            suffix="duplicates",
            where_clause="1=1",
            join_column="id",
            restore_column="value",
            reversibility_score=0.0,
        )

        assert result.backup_sql is not None
        assert result.restore_sql is None

    def test_warnings_for_low_reversibility(self) -> None:
        """Test warnings are added for destructive operations."""
        result = RollbackGenerator.build_full_rollback(
            table="schema.table",
            table_name="table",
            suffix="test",
            where_clause="1=1",
            join_column="id",
            restore_column="value",
            reversibility_score=0.0,
        )

        assert len(result.warnings) > 0
        assert any("DESTRUCTIVE" in w for w in result.warnings)

    def test_warnings_for_medium_reversibility(self) -> None:
        """Test warnings for medium-low reversibility."""
        result = RollbackGenerator.build_full_rollback(
            table="schema.table",
            table_name="table",
            suffix="test",
            where_clause="1=1",
            join_column="id",
            restore_column="value",
            reversibility_score=0.3,
        )

        assert len(result.warnings) > 0
        assert any("lost permanently" in w for w in result.warnings)


class TestGetReversibilityWarning:
    """Tests for RollbackGenerator.get_reversibility_warning()."""

    def test_destructive_warning(self) -> None:
        """Test warning for zero reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(0.0)
        assert warning is not None
        assert "DESTRUCTIVE" in warning

    def test_very_low_warning(self) -> None:
        """Test warning for very low reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(0.2)
        assert warning is not None
        assert "Very low" in warning

    def test_low_warning(self) -> None:
        """Test warning for low reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(0.4)
        assert warning is not None
        assert "Low" in warning

    def test_moderate_warning(self) -> None:
        """Test warning for moderate reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(0.6)
        assert warning is not None
        assert "Moderate" in warning

    def test_no_warning_for_high_reversibility(self) -> None:
        """Test no warning for high reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(0.8)
        assert warning is None

    def test_no_warning_for_full_reversibility(self) -> None:
        """Test no warning for full reversibility."""
        warning = RollbackGenerator.get_reversibility_warning(1.0)
        assert warning is None
