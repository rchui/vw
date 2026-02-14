"""Integration tests for DuckDB operators."""

from tests.utils import sql
from vw.duckdb import col, lit, param, ref, render


def describe_comparison_operators() -> None:
    def it_supports_equality() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE status = 'active'
        """

        q = ref("users").select(col("id")).where(col("status") == lit("active"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_inequality() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE status <> 'banned'
        """

        q = ref("users").select(col("id")).where(col("status") != lit("banned"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_greater_than() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE age > 18
        """

        q = ref("users").select(col("id")).where(col("age") > lit(18))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_less_than_or_equal() -> None:
        expected_sql = """
        SELECT id
        FROM products
        WHERE price <= $max_price
        """

        q = ref("products").select(col("id")).where(col("price") <= param("max_price", 100))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"max_price": 100}


def describe_arithmetic_operators() -> None:
    def it_supports_addition() -> None:
        expected_sql = """
        SELECT price + tax AS total
        FROM products
        """

        q = ref("products").select((col("price") + col("tax")).alias("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_subtraction() -> None:
        expected_sql = """
        SELECT revenue - cost AS profit
        FROM sales
        """

        q = ref("sales").select((col("revenue") - col("cost")).alias("profit"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_multiplication() -> None:
        expected_sql = """
        SELECT quantity * price AS total
        FROM orders
        """

        q = ref("orders").select((col("quantity") * col("price")).alias("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_division() -> None:
        expected_sql = """
        SELECT total / quantity AS avg_price
        FROM orders
        """

        q = ref("orders").select((col("total") / col("quantity")).alias("avg_price"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_logical_operators() -> None:
    def it_supports_and() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE (active) AND (verified)
        """

        q = ref("users").select(col("id")).where(col("active") & col("verified"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_or() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE (premium) OR (trial)
        """

        q = ref("users").select(col("id")).where(col("premium") | col("trial"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_not() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE NOT (banned)
        """

        q = ref("users").select(col("id")).where(~col("banned"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_pattern_matching() -> None:
    def it_supports_like() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE name LIKE '%john%'
        """

        q = ref("users").select(col("id")).where(col("name").like(lit("%john%")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_in() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE status IN ('active', 'pending', 'trial')
        """

        q = ref("users").select(col("id")).where(col("status").is_in(lit("active"), lit("pending"), lit("trial")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_between() -> None:
        expected_sql = """
        SELECT id
        FROM products
        WHERE price BETWEEN 10 AND 100
        """

        q = ref("products").select(col("id")).where(col("price").between(lit(10), lit(100)))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_is_null() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE deleted_at IS NULL
        """

        q = ref("users").select(col("id")).where(col("deleted_at").is_null())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
