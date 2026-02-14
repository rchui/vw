"""Integration tests for DuckDB query building."""

from tests.utils import sql
from vw.duckdb import col, ref, render


def describe_basic_queries() -> None:
    def it_builds_simple_select() -> None:
        expected_sql = """
            SELECT id, name, email
            FROM users
        """

        q = ref("users").select(col("id"), col("name"), col("email"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_select_with_where() -> None:
        expected_sql = """
        SELECT id, name
        FROM users
        WHERE active
        """

        q = ref("users").select(col("id"), col("name")).where(col("active"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_select_with_pagination() -> None:
        expected_sql = """
        SELECT id, name
        FROM users
        ORDER BY id
        LIMIT 10 OFFSET 20
        """

        q = ref("users").select(col("id"), col("name")).order_by(col("id")).offset(20).limit(10)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_filtered_queries() -> None:
    def it_builds_multi_condition_where() -> None:
        expected_sql = """
        SELECT id, name
        FROM users
        WHERE active AND verified AND premium
        """

        q = ref("users").select(col("id"), col("name")).where(col("active"), col("verified"), col("premium"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_filtered_ordered_limited() -> None:
        expected_sql = """
        SELECT id, name, price
        FROM products
        WHERE in_stock AND published
        ORDER BY price, name
        LIMIT 50
        """

        q = (
            ref("products")
            .select(col("id"), col("name"), col("price"))
            .where(col("in_stock"), col("published"))
            .order_by(col("price"), col("name"))
            .limit(50)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_aggregation_queries() -> None:
    def it_builds_simple_group_by() -> None:
        expected_sql = """
        SELECT user_id, total
        FROM orders
        GROUP BY user_id
        """

        q = ref("orders").select(col("user_id"), col("total")).group_by(col("user_id"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_group_by_with_having() -> None:
        expected_sql = """
        SELECT user_id, total
        FROM orders
        GROUP BY user_id
        HAVING total
        """

        q = ref("orders").select(col("user_id"), col("total")).group_by(col("user_id")).having(col("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_distinct() -> None:
    def it_builds_select_distinct() -> None:
        expected_sql = """
        SELECT DISTINCT category
        FROM products
        """

        q = ref("products").select(col("category")).distinct()
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
