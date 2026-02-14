"""Integration tests for DuckDB joins."""

from tests.utils import sql
from vw.duckdb import col, ref, render


def describe_inner_join() -> None:
    def it_supports_inner_join_with_on() -> None:
        expected_sql = """
        SELECT u.id, u.name, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.id"), col("u.name"), col("o.total")).join.inner(
            orders, on=[(col("u.id") == col("o.user_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_inner_join_with_multiple_conditions() -> None:
        expected_sql = """
        SELECT u.id, o.id
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id AND u.tenant_id = o.tenant_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.id"), col("o.id")).join.inner(
            orders, on=[(col("u.id") == col("o.user_id")), (col("u.tenant_id") == col("o.tenant_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_left_join() -> None:
    def it_supports_left_join() -> None:
        expected_sql = """
        SELECT u.id, u.name, o.total
        FROM users AS u
        LEFT JOIN orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.id"), col("u.name"), col("o.total")).join.left(
            orders, on=[(col("u.id") == col("o.user_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_right_join() -> None:
    def it_supports_right_join() -> None:
        expected_sql = """
        SELECT u.id, u.name, o.total
        FROM users AS u
        RIGHT JOIN orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.id"), col("u.name"), col("o.total")).join.right(
            orders, on=[(col("u.id") == col("o.user_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_full_outer_join() -> None:
    def it_supports_full_outer_join() -> None:
        expected_sql = """
        SELECT u.id, u.name, o.total
        FROM users AS u
        FULL JOIN orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.id"), col("u.name"), col("o.total")).join.full_outer(
            orders, on=[(col("u.id") == col("o.user_id"))]
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_cross_join() -> None:
    def it_supports_cross_join() -> None:
        expected_sql = """
        SELECT u.id, p.id
        FROM users AS u
        CROSS JOIN products AS p
        """

        users = ref("users").alias("u")
        products = ref("products").alias("p")
        q = users.select(col("u.id"), col("p.id")).join.cross(products)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_multiple_joins() -> None:
    def it_supports_chaining_joins() -> None:
        expected_sql = """
        SELECT u.name, o.total, p.name
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        INNER JOIN products AS p ON (o.product_id = p.id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        products = ref("products").alias("p")

        q = (
            users.select(col("u.name"), col("o.total"), col("p.name"))
            .join.inner(orders, on=[(col("u.id") == col("o.user_id"))])
            .join.inner(products, on=[(col("o.product_id") == col("p.id"))])
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
