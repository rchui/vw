"""Integration tests for DuckDB window functions."""

from tests.utils import sql
from vw.duckdb import F, col, ref, render


def describe_basic_window_functions() -> None:
    def it_supports_row_number() -> None:
        expected_sql = """
        SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) AS row_num
        FROM users
        """

        q = ref("users").select(
            col("id"), col("name"), F.row_number().over(order_by=[col("created_at")]).alias("row_num")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_rank() -> None:
        expected_sql = """
        SELECT id, score, RANK() OVER (ORDER BY score DESC) AS rank
        FROM players
        """

        q = ref("players").select(col("id"), col("score"), F.rank().over(order_by=[col("score").desc()]).alias("rank"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_dense_rank() -> None:
        expected_sql = """
        SELECT id, score, DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
        FROM players
        """

        q = ref("players").select(
            col("id"), col("score"), F.dense_rank().over(order_by=[col("score").desc()]).alias("dense_rank")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_partitioned_window_functions() -> None:
    def it_supports_partition_by() -> None:
        expected_sql = """
        SELECT category, name, price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rank_in_category
        FROM products
        """

        q = ref("products").select(
            col("category"),
            col("name"),
            col("price"),
            F.row_number()
            .over(partition_by=[col("category")], order_by=[col("price").desc()])
            .alias("rank_in_category"),
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_multiple_partition_columns() -> None:
        expected_sql = """
        SELECT region, category, sales, ROW_NUMBER() OVER (PARTITION BY region, category ORDER BY sales DESC) AS rank
        FROM sales
        """

        q = ref("sales").select(
            col("region"),
            col("category"),
            col("sales"),
            F.row_number()
            .over(partition_by=[col("region"), col("category")], order_by=[col("sales").desc()])
            .alias("rank"),
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_analytic_functions() -> None:
    def it_supports_lag() -> None:
        expected_sql = """
        SELECT date, value, LAG(value) OVER (ORDER BY date) AS prev_value
        FROM metrics
        """

        q = ref("metrics").select(
            col("date"), col("value"), F.lag(col("value")).over(order_by=[col("date")]).alias("prev_value")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_lead() -> None:
        expected_sql = """
        SELECT date, value, LEAD(value) OVER (ORDER BY date) AS next_value
        FROM metrics
        """

        q = ref("metrics").select(
            col("date"), col("value"), F.lead(col("value")).over(order_by=[col("date")]).alias("next_value")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_first_value() -> None:
        expected_sql = """
        SELECT id, value, FIRST_VALUE(value) OVER (ORDER BY date) AS first_val
        FROM metrics
        """

        q = ref("metrics").select(
            col("id"), col("value"), F.first_value(col("value")).over(order_by=[col("date")]).alias("first_val")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_last_value() -> None:
        expected_sql = """
        SELECT id, value, LAST_VALUE(value) OVER (ORDER BY date) AS last_val
        FROM metrics
        """

        q = ref("metrics").select(
            col("id"), col("value"), F.last_value(col("value")).over(order_by=[col("date")]).alias("last_val")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_aggregate_window_functions() -> None:
    def it_supports_sum_over() -> None:
        expected_sql = """
        SELECT date, amount, SUM(amount) OVER (ORDER BY date) AS running_total
        FROM transactions
        """

        q = ref("transactions").select(
            col("date"), col("amount"), F.sum(col("amount")).over(order_by=[col("date")]).alias("running_total")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_supports_avg_over() -> None:
        expected_sql = """
        SELECT id, value, AVG(value) OVER (PARTITION BY category) AS category_avg
        FROM products
        """

        q = ref("products").select(
            col("id"), col("value"), F.avg(col("value")).over(partition_by=[col("category")]).alias("category_avg")
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
