"""Tests for PostgreSQL-specific RowSet methods.

This file tests PostgreSQL-specific RowSet features:
- fetch() with WITH TIES support
- modifiers() for row-level locking (FOR UPDATE, FOR SHARE, etc.)

Core ANSI SQL RowSet methods (col, star, where, group_by, having, order_by, limit,
offset, distinct) are tested in tests/core/test_base.py.
"""

from vw.postgres import col, ref, render


def describe_fetch() -> None:
    def it_renders_fetch_first() -> None:
        """fetch() should render FETCH FIRST."""
        q = ref("users").select(col("*")).fetch(10)
        result = render(q)
        assert result.query == "SELECT * FROM users FETCH FIRST 10 ROWS ONLY"

    def it_renders_fetch_with_ties() -> None:
        """fetch() with with_ties should render WITH TIES."""
        q = ref("scores").select(col("*")).order_by(col("score").desc()).fetch(5, with_ties=True)
        result = render(q)
        assert result.query == "SELECT * FROM scores ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES"

    def it_works_with_offset() -> None:
        """fetch() should work with offset()."""
        q = ref("users").select(col("*")).offset(10).fetch(5)
        result = render(q)
        assert result.query == "SELECT * FROM users OFFSET 10 FETCH FIRST 5 ROWS ONLY"

    def it_works_with_order_by_and_offset() -> None:
        """fetch() should work with ORDER BY and OFFSET."""
        q = ref("products").select(col("*")).order_by(col("name")).offset(20).fetch(10)
        result = render(q)
        assert result.query == "SELECT * FROM products ORDER BY name OFFSET 20 FETCH FIRST 10 ROWS ONLY"

    def it_replaces_previous_fetch() -> None:
        """Multiple fetch() calls - last wins."""
        q = ref("users").select(col("*")).fetch(5).fetch(10)
        result = render(q)
        assert result.query == "SELECT * FROM users FETCH FIRST 10 ROWS ONLY"


def describe_modifiers() -> None:
    def describe_statement_level() -> None:
        def it_renders_for_update() -> None:
            """Modifier on statement should render at end."""
            from vw.postgres import raw

            q = ref("users").select(col("*")).modifiers(raw.expr("FOR UPDATE"))
            result = render(q)
            assert result.query == "SELECT * FROM users FOR UPDATE"

        def it_renders_for_update_nowait() -> None:
            """FOR UPDATE NOWAIT modifier."""
            from vw.postgres import raw

            q = ref("users").select(col("id")).modifiers(raw.expr("FOR UPDATE NOWAIT"))
            result = render(q)
            assert result.query == "SELECT id FROM users FOR UPDATE NOWAIT"

        def it_renders_for_update_skip_locked() -> None:
            """FOR UPDATE SKIP LOCKED modifier."""
            from vw.postgres import param, raw

            q = (
                ref("jobs")
                .select(col("*"))
                .where(col("status") == param("s", "pending"))
                .modifiers(raw.expr("FOR UPDATE SKIP LOCKED"))
            )
            result = render(q)
            assert result.query == "SELECT * FROM jobs WHERE status = $s FOR UPDATE SKIP LOCKED"
            assert result.params == {"s": "pending"}

        def it_renders_after_limit() -> None:
            """Modifier should render after LIMIT."""
            from vw.postgres import raw

            q = ref("jobs").select(col("*")).limit(10).modifiers(raw.expr("FOR UPDATE"))
            result = render(q)
            assert result.query == "SELECT * FROM jobs LIMIT 10 FOR UPDATE"

        def it_renders_after_offset_and_limit() -> None:
            """Modifier should render after LIMIT OFFSET."""
            from vw.postgres import raw

            q = ref("items").select(col("*")).offset(20).limit(10).modifiers(raw.expr("FOR SHARE"))
            result = render(q)
            assert result.query == "SELECT * FROM items LIMIT 10 OFFSET 20 FOR SHARE"

        def it_renders_after_fetch() -> None:
            """Modifier should render after FETCH."""
            from vw.postgres import raw

            q = ref("users").select(col("*")).fetch(10).modifiers(raw.expr("FOR UPDATE"))
            result = render(q)
            assert result.query == "SELECT * FROM users FETCH FIRST 10 ROWS ONLY FOR UPDATE"

        def it_accumulates_multiple_modifiers() -> None:
            """Multiple modifiers accumulate."""
            from vw.postgres import raw

            q = ref("users").select(col("*")).modifiers(raw.expr("FOR UPDATE"), raw.expr("SKIP LOCKED"))
            result = render(q)
            assert result.query == "SELECT * FROM users FOR UPDATE SKIP LOCKED"
