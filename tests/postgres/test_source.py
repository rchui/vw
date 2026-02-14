"""Tests for PostgreSQL-specific ref() features.

This file tests PostgreSQL-specific features of the ref() factory function
and Reference state, particularly the modifiers() method for TABLESAMPLE,
PARTITION clauses, and other table-level modifiers. Core ANSI SQL behavior
is tested in tests/core/test_source.py.
"""

from vw.postgres import col, param, raw, ref, render


def describe_modifiers() -> None:
    def describe_table_level() -> None:
        def it_renders_tablesample() -> None:
            """Modifier on table should render between name and alias."""
            q = ref("users").modifiers(raw.expr("TABLESAMPLE SYSTEM(5)")).select(col("*"))
            result = render(q)
            assert result.query == "SELECT * FROM users TABLESAMPLE SYSTEM(5)"

        def it_renders_tablesample_with_parameter() -> None:
            """Modifier can use parameters."""
            q = (
                ref("users")
                .modifiers(raw.expr("TABLESAMPLE SYSTEM({pct})", pct=param("sample_pct", 10)))
                .select(col("id"))
            )
            result = render(q)
            assert result.query == "SELECT id FROM users TABLESAMPLE SYSTEM($sample_pct)"
            assert result.params == {"sample_pct": 10}

        def it_renders_partition_clause() -> None:
            """Partition modifier renders correctly."""
            q = ref("sales").modifiers(raw.expr("PARTITION (p2023, p2024)")).select(col("*"))
            result = render(q)
            assert result.query == "SELECT * FROM sales PARTITION (p2023, p2024)"

        def it_renders_modifier_before_alias() -> None:
            """Modifier should render before alias."""
            q = ref("users").modifiers(raw.expr("TABLESAMPLE BERNOULLI(5)")).alias("u").select(col("u.id"))
            result = render(q)
            assert result.query == "SELECT u.id FROM users TABLESAMPLE BERNOULLI(5) AS u"

        def it_accumulates_multiple_modifiers() -> None:
            """Multiple modifiers accumulate in order."""
            q = (
                ref("events")
                .modifiers(raw.expr("TABLESAMPLE SYSTEM(10)"), raw.expr("REPEATABLE(123)"))
                .select(col("*"))
            )
            result = render(q)
            assert result.query == "SELECT * FROM events TABLESAMPLE SYSTEM(10) REPEATABLE(123)"
