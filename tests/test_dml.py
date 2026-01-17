"""Unit tests for DML (INSERT, UPDATE, DELETE)."""

import vw
from vw.dml import Delete, Insert


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


def describe_delete():
    """Tests for Delete class."""

    def it_renders_basic_delete(render_context: vw.RenderContext) -> None:
        result = Delete(table="users").__vw_render__(render_context)
        assert result == "DELETE FROM users"

    def it_renders_delete_with_where(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users")
            .where(vw.col("id") == vw.param("id", 1))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users WHERE (id = $id)"
        assert render_context.params == {"id": 1}

    def it_renders_delete_with_multiple_where(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users")
            .where(vw.col("active") == vw.col("false"), vw.col("age") < vw.col("18"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users WHERE (active = false) AND (age < 18)"

    def it_renders_delete_with_returning(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users")
            .where(vw.col("id") == vw.param("id", 1))
            .returning(vw.col("id"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users WHERE (id = $id) RETURNING id"

    def it_renders_delete_with_returning_star(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users")
            .where(vw.col("id") == vw.param("id", 1))
            .returning(vw.col("*"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users WHERE (id = $id) RETURNING *"

    def it_renders_delete_with_using_table(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users", _using=vw.Source(name="orders").alias("o"))
            .where(vw.col("users.id") == vw.col("o.user_id"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users USING orders AS o WHERE (users.id = o.user_id)"

    def it_renders_delete_with_using_subquery(render_context: vw.RenderContext) -> None:
        subquery = (
            vw.Source(name="orders")
            .select(vw.col("user_id"))
            .where(vw.col("status") == vw.col("'cancelled'"))
            .alias("o")
        )
        result = (
            Delete(table="users", _using=subquery)
            .where(vw.col("users.id") == vw.col("o.user_id"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users USING (SELECT user_id FROM orders WHERE (status = 'cancelled')) AS o WHERE (users.id = o.user_id)"

    def it_renders_delete_with_using_values(render_context: vw.RenderContext) -> None:
        result = (
            Delete(table="users", _using=vw.values({"id": 1}, {"id": 2}).alias("v"))
            .where(vw.col("users.id") == vw.col("v.id"))
            .__vw_render__(render_context)
        )
        assert result == "DELETE FROM users USING (VALUES ($_v0_0_id), ($_v1_0_id)) AS v(id) WHERE (users.id = v.id)"
        assert render_context.params == {"_v0_0_id": 1, "_v1_0_id": 2}


def describe_delete_via_source():
    """Tests for Source.delete() method."""

    def it_creates_basic_delete() -> None:
        delete = vw.Source(name="users").delete()
        assert isinstance(delete, Delete)
        assert delete.table == "users"
        assert delete._using is None

    def it_creates_delete_with_using() -> None:
        using = vw.Source(name="orders").alias("o")
        delete = vw.Source(name="users").delete(using)
        assert isinstance(delete, Delete)
        assert delete.table == "users"
        assert delete._using is using
