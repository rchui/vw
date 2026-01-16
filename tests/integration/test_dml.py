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

    def it_inserts_from_select_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO active_users SELECT * FROM users WHERE (active = true)
        """
        result = (
            vw.Source(name="active_users")
            .insert(
                vw.Source(name="users")
                .select(vw.col("*"))
                .where(vw.col("active") == vw.col("true"))
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_inserts_from_select_with_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            INSERT INTO user_orders
            SELECT users.id, orders.total
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            vw.Source(name="user_orders")
            .insert(
                users.join.inner(orders, on=[vw.col("users.id") == vw.col("orders.user_id")])
                .select(vw.col("users.id"), vw.col("orders.total"))
            )
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
