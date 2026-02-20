"""Tests for PostgreSQL-specific RowSet methods.

This file tests PostgreSQL-specific RowSet features:
- fetch() with WITH TIES support
- modifiers() for row-level locking (FOR UPDATE, FOR SHARE, etc.)

Core ANSI SQL RowSet methods (col, star, where, group_by, having, order_by, limit,
offset, distinct) are tested in tests/core/test_base.py.
"""

from vw.postgres import col, modifiers, ref, render
from vw.postgres.states import RowLock


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
    def describe_row_lock_factory() -> None:
        def it_creates_row_lock_state() -> None:
            expr = modifiers.row_lock("UPDATE")
            assert isinstance(expr.state, RowLock)
            assert expr.state.strength == "UPDATE"
            assert expr.state.wait_policy is None

        def it_renders_for_update() -> None:
            q = ref("users").select(col("*")).modifiers(modifiers.row_lock("UPDATE"))
            assert render(q).query == "SELECT * FROM users FOR UPDATE"

        def it_renders_for_share() -> None:
            q = ref("users").select(col("*")).modifiers(modifiers.row_lock("SHARE"))
            assert render(q).query == "SELECT * FROM users FOR SHARE"

        def it_renders_for_no_key_update() -> None:
            q = ref("users").select(col("*")).modifiers(modifiers.row_lock("NO KEY UPDATE"))
            assert render(q).query == "SELECT * FROM users FOR NO KEY UPDATE"

        def it_renders_for_key_share() -> None:
            q = ref("users").select(col("*")).modifiers(modifiers.row_lock("KEY SHARE"))
            assert render(q).query == "SELECT * FROM users FOR KEY SHARE"

        def it_renders_nowait() -> None:
            q = ref("users").select(col("id")).modifiers(modifiers.row_lock("UPDATE", wait_policy="NOWAIT"))
            assert render(q).query == "SELECT id FROM users FOR UPDATE NOWAIT"

        def it_renders_skip_locked() -> None:
            from vw.postgres import param

            q = (
                ref("jobs")
                .select(col("*"))
                .where(col("status") == param("s", "pending"))
                .modifiers(modifiers.row_lock("UPDATE", wait_policy="SKIP LOCKED"))
            )
            result = render(q)
            assert result.query == "SELECT * FROM jobs WHERE status = $s FOR UPDATE SKIP LOCKED"
            assert result.params == {"s": "pending"}

        def it_renders_of_tables() -> None:
            users = ref("users")
            q = ref("users").select(col("*")).modifiers(modifiers.row_lock("UPDATE", of=(users.state,)))
            assert render(q).query == "SELECT * FROM users FOR UPDATE OF users"

        def it_renders_of_tables_with_alias() -> None:
            users = ref("users").alias("u")
            q = ref("users").alias("u").select(col("*")).modifiers(modifiers.row_lock("UPDATE", of=(users.state,)))
            assert render(q).query == "SELECT * FROM users AS u FOR UPDATE OF u"

        def it_renders_after_limit() -> None:
            q = ref("jobs").select(col("*")).limit(10).modifiers(modifiers.row_lock("UPDATE"))
            assert render(q).query == "SELECT * FROM jobs LIMIT 10 FOR UPDATE"

        def it_renders_after_offset_and_limit() -> None:
            q = ref("items").select(col("*")).offset(20).limit(10).modifiers(modifiers.row_lock("SHARE"))
            assert render(q).query == "SELECT * FROM items LIMIT 10 OFFSET 20 FOR SHARE"

        def it_renders_after_fetch() -> None:
            q = ref("users").select(col("*")).fetch(10).modifiers(modifiers.row_lock("UPDATE"))
            assert render(q).query == "SELECT * FROM users FETCH FIRST 10 ROWS ONLY FOR UPDATE"
