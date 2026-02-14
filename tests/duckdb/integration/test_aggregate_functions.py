"""Integration tests for DuckDB aggregate functions."""

from tests.utils import sql
from vw.duckdb import F, col, lit, ref, render


def describe_basic_aggregates() -> None:
    def it_supports_count() -> None:
        expected_sql = """
        SELECT COUNT(*) AS total
        FROM users
        """

        q = ref("users").select(F.count().alias("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_count_column() -> None:
        expected_sql = """
        SELECT COUNT(id) AS total
        FROM users
        """

        q = ref("users").select(F.count(col("id")).alias("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_count_distinct() -> None:
        expected_sql = """
        SELECT COUNT(DISTINCT user_id) AS unique_users
        FROM orders
        """

        q = ref("orders").select(F.count(col("user_id"), distinct=True).alias("unique_users"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_sum() -> None:
        expected_sql = """
        SELECT SUM(amount) AS total_amount
        FROM orders
        """

        q = ref("orders").select(F.sum(col("amount")).alias("total_amount"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_avg() -> None:
        expected_sql = """
        SELECT AVG(price) AS avg_price
        FROM products
        """

        q = ref("products").select(F.avg(col("price")).alias("avg_price"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_min_max() -> None:
        expected_sql = """
        SELECT MIN(price) AS min_price, MAX(price) AS max_price
        FROM products
        """

        q = ref("products").select(F.min(col("price")).alias("min_price"), F.max(col("price")).alias("max_price"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_grouped_aggregates() -> None:
    def it_supports_group_by_with_count() -> None:
        expected_sql = """
        SELECT category, COUNT(*) AS count
        FROM products
        GROUP BY category
        """

        q = ref("products").select(col("category"), F.count().alias("count")).group_by(col("category"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_group_by_with_sum() -> None:
        expected_sql = """
        SELECT user_id, SUM(amount) AS total
        FROM orders
        GROUP BY user_id
        """

        q = ref("orders").select(col("user_id"), F.sum(col("amount")).alias("total")).group_by(col("user_id"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_group_by_with_having() -> None:
        expected_sql = """
        SELECT user_id, COUNT(*) AS order_count
        FROM orders
        GROUP BY user_id
        HAVING COUNT(*) > 5
        """

        q = (
            ref("orders")
            .select(col("user_id"), F.count().alias("order_count"))
            .group_by(col("user_id"))
            .having(F.count() > lit(5))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_filter_clause() -> None:
    def it_supports_filter_on_count() -> None:
        expected_sql = """
        SELECT COUNT(*) FILTER (WHERE active) AS active_count
        FROM users
        """

        q = ref("users").select(F.count().filter(col("active")).alias("active_count"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_filter_on_sum() -> None:
        expected_sql = """
        SELECT SUM(amount) FILTER (WHERE status = 'completed') AS completed_total
        FROM orders
        """

        q = ref("orders").select(
            F.sum(col("amount")).filter(col("status") == lit("completed")).alias("completed_total")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
