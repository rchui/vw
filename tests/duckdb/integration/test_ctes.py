"""Integration tests for DuckDB CTEs (Common Table Expressions)."""

from tests.utils import sql
from vw.duckdb import F, col, cte, lit, ref, render


def describe_basic_ctes() -> None:
    def it_supports_simple_cte() -> None:
        expected_sql = """
        WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE)
        SELECT id, name FROM active_users
        """

        active_users = cte(
            "active_users", ref("users").select(col("id"), col("name")).where(col("active") == lit(True))
        )
        q = active_users.select(col("id"), col("name"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_cte_with_aggregation() -> None:
        expected_sql = """
        WITH order_counts AS (SELECT user_id, COUNT(*) AS order_count FROM orders GROUP BY user_id)
        SELECT user_id, order_count FROM order_counts WHERE order_count > 5
        """

        order_counts = cte(
            "order_counts",
            ref("orders").select(col("user_id"), F.count().alias("order_count")).group_by(col("user_id")),
        )
        q = order_counts.select(col("user_id"), col("order_count")).where(col("order_count") > lit(5))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_multiple_ctes() -> None:
    def it_supports_multiple_ctes() -> None:
        expected_sql = """
        WITH active_users AS (SELECT id, name FROM users WHERE active = TRUE),
        premium_users AS (SELECT id, name FROM active_users WHERE premium = TRUE)
        SELECT id, name FROM premium_users
        """

        active_users = cte(
            "active_users", ref("users").select(col("id"), col("name")).where(col("active") == lit(True))
        )
        premium_users = cte(
            "premium_users", active_users.select(col("id"), col("name")).where(col("premium") == lit(True))
        )
        q = premium_users.select(col("id"), col("name"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_cte_with_joins() -> None:
    def it_supports_cte_with_join() -> None:
        expected_sql = """
        WITH user_orders AS (SELECT user_id, COUNT(*) AS order_count FROM orders GROUP BY user_id)
        SELECT u.id, u.name, uo.order_count
        FROM users AS u
        INNER JOIN user_orders AS uo ON (u.id = uo.user_id)
        """

        user_orders = cte(
            "user_orders",
            ref("orders").select(col("user_id"), F.count().alias("order_count")).group_by(col("user_id")),
        )
        users = ref("users").alias("u")
        user_orders_ref = user_orders.alias("uo")

        q = users.select(col("u.id"), col("u.name"), col("uo.order_count")).join.inner(
            user_orders_ref, on=[(col("u.id") == col("uo.user_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_recursive_ctes() -> None:
    def it_supports_recursive_cte() -> None:
        expected_sql = """
        WITH RECURSIVE subordinates AS (SELECT id, name, manager_id FROM employees WHERE id = 1)
        SELECT id, name FROM subordinates
        """

        subordinates = cte(
            "subordinates",
            ref("employees").select(col("id"), col("name"), col("manager_id")).where(col("id") == lit(1)),
            recursive=True,
        )
        q = subordinates.select(col("id"), col("name"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
