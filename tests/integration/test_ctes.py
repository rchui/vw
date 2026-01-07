"""Integration tests for Common Table Expressions (CTEs)."""

import vw
from tests.utils import sql


def describe_basic_ctes():
    """Tests for basic CTE functionality."""

    def it_generates_basic_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (status = 'active'))
            SELECT * FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_qualified_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT id, name FROM users)
            SELECT active_users.id, active_users.name FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")),
        )
        result = active_users.select(active_users.col("id"), active_users.col("name")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (status = :status))
            SELECT * FROM active_users
        """
        status = vw.param("status", "active")
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == status),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"status": "active"})


def describe_cte_with_joins():
    """Tests for CTEs used in joins."""

    def it_generates_cte_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT id, name FROM users WHERE (active = true))
            SELECT orders.id, active_users.name
            FROM orders
            INNER JOIN active_users ON (orders.user_id = active_users.id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")).where(vw.col("active") == vw.col("true")),
        )
        orders = vw.Source(name="orders")
        result = (
            orders.join.inner(active_users, on=[orders.col("user_id") == active_users.col("id")])
            .select(orders.col("id"), active_users.col("name"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_multiple_ctes():
    """Tests for multiple CTEs."""

    def it_generates_multiple_ctes(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (active = true)),
                 recent_orders AS (SELECT * FROM orders WHERE (created_at > '2024-01-01'))
            SELECT *
            FROM active_users
            INNER JOIN recent_orders ON (active_users.id = recent_orders.user_id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        recent_orders = vw.cte(
            "recent_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("created_at") > vw.col("'2024-01-01'")),
        )
        result = (
            active_users.join.inner(recent_orders, on=[active_users.col("id") == recent_orders.col("user_id")])
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_referencing_another_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH base_users AS (SELECT * FROM users),
                 active_users AS (SELECT * FROM base_users WHERE (active = true))
            SELECT * FROM active_users
        """
        base_users = vw.cte(
            "base_users",
            vw.Source(name="users").select(vw.col("*")),
        )
        active_users = vw.cte(
            "active_users",
            base_users.select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_cte_with_clauses():
    """Tests for CTEs with various SQL clauses."""

    def it_generates_cte_with_group_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH customer_totals AS (SELECT customer_id, SUM(total) AS total FROM orders GROUP BY customer_id)
            SELECT * FROM customer_totals
        """
        customer_totals = vw.cte(
            "customer_totals",
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)").alias("total"))
            .group_by(vw.col("customer_id")),
        )
        result = customer_totals.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_having(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_orders AS (SELECT * FROM orders WHERE (status = 'active'))
            SELECT customer_id, SUM(total)
            FROM active_orders
            GROUP BY customer_id
            HAVING (SUM(total) > 500)
        """
        active_orders = vw.cte(
            "active_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = (
            active_orders.select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("SUM(total)") > vw.col("500"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_limit(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (active = true))
            SELECT * FROM active_users ORDER BY id ASC LIMIT 10
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = active_users.select(vw.col("*")).order_by(vw.col("id").asc()).limit(10).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
