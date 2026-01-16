"""Unit tests for DML (INSERT, UPDATE, DELETE)."""

import vw
from vw.dml import Insert


def describe_insert():
    """Tests for Insert class."""

    def it_renders_insert_with_single_row(render_context: vw.RenderContext) -> None:
        result = Insert(
            table="users",
            source=vw.values({"name": "Alice", "age": 30}),
        ).__vw_render__(render_context)
        assert result == "INSERT INTO users (name, age) VALUES ($_v0_0_name, $_v0_1_age)"
        assert render_context.params == {"_v0_0_name": "Alice", "_v0_1_age": 30}

    def it_renders_insert_with_multiple_rows(render_context: vw.RenderContext) -> None:
        result = Insert(
            table="users",
            source=vw.values(
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ),
        ).__vw_render__(render_context)
        assert result == "INSERT INTO users (name, age) VALUES ($_v0_0_name, $_v0_1_age), ($_v1_0_name, $_v1_1_age)"
        assert render_context.params == {
            "_v0_0_name": "Alice",
            "_v0_1_age": 30,
            "_v1_0_name": "Bob",
            "_v1_1_age": 25,
        }

    def it_renders_insert_from_select(render_context: vw.RenderContext) -> None:
        query = vw.Source(name="users").select(vw.col("name"), vw.col("age"))
        result = Insert(table="users_backup", source=query).__vw_render__(render_context)
        assert result == "INSERT INTO users_backup SELECT name, age FROM users"

    def it_renders_insert_with_returning(render_context: vw.RenderContext) -> None:
        result = (
            Insert(table="users", source=vw.values({"name": "Alice"}))
            .returning(vw.col("id"))
            .__vw_render__(render_context)
        )
        assert result == "INSERT INTO users (name) VALUES ($_v0_0_name) RETURNING id"

    def it_renders_insert_with_multiple_returning(render_context: vw.RenderContext) -> None:
        result = (
            Insert(table="users", source=vw.values({"name": "Alice"}))
            .returning(vw.col("id"), vw.col("created_at"))
            .__vw_render__(render_context)
        )
        assert result == "INSERT INTO users (name) VALUES ($_v0_0_name) RETURNING id, created_at"

    def it_renders_insert_with_expression_values(render_context: vw.RenderContext) -> None:
        result = Insert(
            table="users",
            source=vw.values({"name": "Alice", "created_at": vw.col("NOW()")}),
        ).__vw_render__(render_context)
        assert result == "INSERT INTO users (name, created_at) VALUES ($_v0_0_name, NOW())"

    def it_renders_insert_with_returning_star(render_context: vw.RenderContext) -> None:
        result = (
            Insert(table="users", source=vw.values({"name": "Alice"}))
            .returning(vw.col("*"))
            .__vw_render__(render_context)
        )
        assert result == "INSERT INTO users (name) VALUES ($_v0_0_name) RETURNING *"


def describe_insert_via_source():
    """Tests for Source.insert() method."""

    def it_creates_insert_from_values() -> None:
        insert = vw.Source(name="users").insert(vw.values({"name": "Alice"}))
        assert isinstance(insert, Insert)
        assert insert.table == "users"

    def it_creates_insert_from_select() -> None:
        query = vw.Source(name="users").select(vw.col("*"))
        insert = vw.Source(name="users_backup").insert(query)
        assert isinstance(insert, Insert)
        assert insert.table == "users_backup"
