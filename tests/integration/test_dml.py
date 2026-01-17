"""Integration tests for DML (INSERT, UPDATE, DELETE)."""

import vw
from tests.utils import sql


def describe_insert_with_values():
    """Tests for INSERT with VALUES."""

    def it_inserts_single_row(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name, age) VALUES ($_v0_0_name, $_v0_1_age)
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice", "age": 30}))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice", "_v0_1_age": 30},
        )

    def it_inserts_multiple_rows(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name, age)
            VALUES ($_v0_0_name, $_v0_1_age), ($_v1_0_name, $_v1_1_age), ($_v2_0_name, $_v2_1_age)
        """
        result = (
            vw.Source(name="users")
            .insert(
                vw.values(
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": 25},
                    {"name": "Charlie", "age": 35},
                )
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={
                "_v0_0_name": "Alice",
                "_v0_1_age": 30,
                "_v1_0_name": "Bob",
                "_v1_1_age": 25,
                "_v2_0_name": "Charlie",
                "_v2_1_age": 35,
            },
        )

    def it_inserts_with_none_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name, email) VALUES ($_v0_0_name, $_v0_1_email)
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice", "email": None}))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice", "_v0_1_email": None},
        )

    def it_inserts_with_expressions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name, created_at) VALUES ($_v0_0_name, NOW())
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice", "created_at": vw.col("NOW()")}))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice"},
        )


def describe_insert_from_select():
    """Tests for INSERT ... SELECT."""

    def it_inserts_from_simple_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users_backup SELECT name, age FROM users
        """
        result = (
            vw.Source(name="users_backup")
            .insert(vw.Source(name="users").select(vw.col("name"), vw.col("age")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_delete_basic():
    """Tests for basic DELETE statements."""

    def it_deletes_all_rows(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM temp_data
        """
        result = vw.Source(name="temp_data").delete().render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_deletes_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users WHERE (id = $id)
        """
        result = (
            vw.Source(name="users")
            .delete()
            .where(vw.col("id") == vw.param("id", 1))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"id": 1})

    def it_deletes_with_complex_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users WHERE (active = false) AND (last_login < $cutoff)
        """
        result = (
            vw.Source(name="users")
            .delete()
            .where(
                vw.col("active") == vw.col("false"),
                vw.col("last_login") < vw.param("cutoff", "2024-01-01"),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"cutoff": "2024-01-01"},
        )


def describe_delete_with_returning():
    """Tests for DELETE ... RETURNING."""

    def it_returns_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users WHERE (id = $id) RETURNING id
        """
        result = (
            vw.Source(name="users")
            .delete()
            .where(vw.col("id") == vw.param("id", 1))
            .returning(vw.col("id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"id": 1})

    def it_returns_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users WHERE (id = $id) RETURNING id, name, email
        """
        result = (
            vw.Source(name="users")
            .delete()
            .where(vw.col("id") == vw.param("id", 1))
            .returning(vw.col("id"), vw.col("name"), vw.col("email"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"id": 1})

    def it_returns_star(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users WHERE (id = $id) RETURNING *
        """
        result = (
            vw.Source(name="users")
            .delete()
            .where(vw.col("id") == vw.param("id", 1))
            .returning(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"id": 1})


def describe_delete_with_using():
    """Tests for DELETE ... USING."""

    def it_deletes_using_table(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users
            USING orders AS o
            WHERE (users.id = o.user_id)
        """
        result = (
            vw.Source(name="users")
            .delete(vw.Source(name="orders").alias("o"))
            .where(vw.col("users.id") == vw.col("o.user_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_deletes_using_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users
            USING (SELECT user_id FROM orders WHERE (status = 'cancelled')) AS o
            WHERE (users.id = o.user_id)
        """
        subquery = (
            vw.Source(name="orders")
            .select(vw.col("user_id"))
            .where(vw.col("status") == vw.col("'cancelled'"))
            .alias("o")
        )
        result = (
            vw.Source(name="users")
            .delete(subquery)
            .where(vw.col("users.id") == vw.col("o.user_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_deletes_using_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users
            USING (VALUES ($_v0_0_id), ($_v1_0_id), ($_v2_0_id)) AS v(id)
            WHERE (users.id = v.id)
        """
        result = (
            vw.Source(name="users")
            .delete(vw.values({"id": 1}, {"id": 2}, {"id": 3}).alias("v"))
            .where(vw.col("users.id") == vw.col("v.id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v1_0_id": 2, "_v2_0_id": 3},
        )

    def it_deletes_using_with_returning(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            DELETE FROM users
            USING orders AS o
            WHERE (users.id = o.user_id)
            RETURNING users.*
        """
        result = (
            vw.Source(name="users")
            .delete(vw.Source(name="orders").alias("o"))
            .where(vw.col("users.id") == vw.col("o.user_id"))
            .returning(vw.col("users.*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_delete_with_cte():
    """Tests for DELETE with CTEs."""

    def it_deletes_using_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH inactive AS (SELECT id FROM users WHERE (active = false))
            DELETE FROM users
            USING inactive
            WHERE (users.id = inactive.id)
        """
        inactive_cte = vw.cte(
            "inactive",
            vw.Source(name="users").select(vw.col("id")).where(vw.col("active") == vw.col("false")),
        )
        result = (
            vw.Source(name="users")
            .delete(inactive_cte)
            .where(vw.col("users.id") == vw.col("inactive.id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_insert_with_returning():
    """Tests for INSERT ... RETURNING."""

    def it_returns_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name) VALUES ($_v0_0_name) RETURNING id
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice"}))
            .returning(vw.col("id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice"},
        )

    def it_returns_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name) VALUES ($_v0_0_name) RETURNING id, created_at
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice"}))
            .returning(vw.col("id"), vw.col("created_at"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice"},
        )

    def it_returns_star(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users (name, age) VALUES ($_v0_0_name, $_v0_1_age) RETURNING *
        """
        result = (
            vw.Source(name="users")
            .insert(vw.values({"name": "Alice", "age": 30}))
            .returning(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_name": "Alice", "_v0_1_age": 30},
        )

    def it_returns_from_insert_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO users_backup SELECT * FROM users RETURNING id
        """
        result = (
            vw.Source(name="users_backup")
            .insert(vw.Source(name="users").select(vw.col("*")))
            .returning(vw.col("id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_insert_with_cte():
    """Tests for INSERT with CTEs."""

    def it_inserts_from_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active AS (SELECT * FROM users WHERE (active = true))
            INSERT INTO active_users SELECT * FROM active
        """
        active_cte = vw.cte(
            "active",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = (
            vw.Source(name="active_users")
            .insert(active_cte.select(vw.col("*")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
