"""Integration tests for window functions."""

from tests.utils import sql
from vw.postgres import F, col, param, render, source


def describe_window_functions():
    """Test window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE)."""

    def describe_row_number():
        """Test ROW_NUMBER window function."""

        def test_row_number_with_order_by():
            expected_sql = """
                SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) FROM orders
            """

            q = source("orders").select(col("id"), F.row_number().over(order_by=[col("created_at").desc()]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_row_number_with_partition_by():
            expected_sql = """
                SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id) AS row_num FROM orders
            """

            q = source("orders").select(
                col("id"), F.row_number().over(partition_by=[col("customer_id")]).alias("row_num")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_row_number_with_partition_and_order():
            expected_sql = """
                SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS order_rank FROM orders
            """

            q = source("orders").select(
                col("id"),
                F.row_number()
                .over(partition_by=[col("customer_id")], order_by=[col("created_at").desc()])
                .alias("order_rank"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_rank():
        """Test RANK window function."""

        def test_rank_with_order_by():
            expected_sql = """
                SELECT id, RANK() OVER (ORDER BY score DESC) AS rank FROM students
            """

            q = source("students").select(col("id"), F.rank().over(order_by=[col("score").desc()]).alias("rank"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_rank_with_partition_and_order():
            expected_sql = """
                SELECT id, RANK() OVER (PARTITION BY class ORDER BY score DESC) AS class_rank FROM students
            """

            q = source("students").select(
                col("id"),
                F.rank().over(partition_by=[col("class")], order_by=[col("score").desc()]).alias("class_rank"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_dense_rank():
        """Test DENSE_RANK window function."""

        def test_dense_rank_with_order_by():
            expected_sql = """
                SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank FROM students
            """

            q = source("students").select(
                col("id"), F.dense_rank().over(order_by=[col("score").desc()]).alias("dense_rank")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_dense_rank_with_partition_and_order():
            expected_sql = """
                SELECT id, DENSE_RANK() OVER (PARTITION BY class ORDER BY score DESC) AS class_dense_rank FROM students
            """

            q = source("students").select(
                col("id"),
                F.dense_rank()
                .over(partition_by=[col("class")], order_by=[col("score").desc()])
                .alias("class_dense_rank"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_ntile():
        """Test NTILE window function."""

        def test_ntile_with_order_by():
            expected_sql = """
                SELECT id, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM employees
            """

            q = source("employees").select(
                col("id"), F.ntile(4).over(order_by=[col("salary").desc()]).alias("quartile")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_ntile_with_partition_and_order():
            expected_sql = """
                SELECT id, NTILE(10) OVER (PARTITION BY department ORDER BY salary DESC) AS decile FROM employees
            """

            q = source("employees").select(
                col("id"),
                F.ntile(10).over(partition_by=[col("department")], order_by=[col("salary").desc()]).alias("decile"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_lag():
        """Test LAG window function."""

        def test_lag_basic():
            expected_sql = """
                SELECT date, LAG(price) OVER (ORDER BY date ASC) AS prev_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.lag(col("price")).over(order_by=[col("date").asc()]).alias("prev_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_lag_with_offset():
            expected_sql = """
                SELECT date, LAG(price, 2) OVER (ORDER BY date ASC) AS price_2_days_ago FROM prices
            """

            q = source("prices").select(
                col("date"), F.lag(col("price"), 2).over(order_by=[col("date").asc()]).alias("price_2_days_ago")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_lag_with_default():
            expected_sql = """
                SELECT date, LAG(price, 1, $default_price) OVER (ORDER BY date ASC) AS prev_price FROM prices
            """

            q = source("prices").select(
                col("date"),
                F.lag(col("price"), 1, param("default_price", 0))
                .over(order_by=[col("date").asc()])
                .alias("prev_price"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"default_price": 0}

        def test_lag_with_partition():
            expected_sql = """
                SELECT symbol, date, LAG(price) OVER (PARTITION BY symbol ORDER BY date ASC) AS prev_price FROM prices
            """

            q = source("prices").select(
                col("symbol"),
                col("date"),
                F.lag(col("price"))
                .over(partition_by=[col("symbol")], order_by=[col("date").asc()])
                .alias("prev_price"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_lead():
        """Test LEAD window function."""

        def test_lead_basic():
            expected_sql = """
                SELECT date, LEAD(price) OVER (ORDER BY date ASC) AS next_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.lead(col("price")).over(order_by=[col("date").asc()]).alias("next_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_lead_with_offset():
            expected_sql = """
                SELECT date, LEAD(price, 3) OVER (ORDER BY date ASC) AS price_3_days_later FROM prices
            """

            q = source("prices").select(
                col("date"), F.lead(col("price"), 3).over(order_by=[col("date").asc()]).alias("price_3_days_later")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_lead_with_default():
            expected_sql = """
                SELECT date, LEAD(price, 1, $default_price) OVER (ORDER BY date ASC) AS next_price FROM prices
            """

            q = source("prices").select(
                col("date"),
                F.lead(col("price"), 1, param("default_price", 0))
                .over(order_by=[col("date").asc()])
                .alias("next_price"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"default_price": 0}

    def describe_first_value():
        """Test FIRST_VALUE window function."""

        def test_first_value():
            expected_sql = """
                SELECT date, FIRST_VALUE(price) OVER (ORDER BY date ASC) AS first_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.first_value(col("price")).over(order_by=[col("date").asc()]).alias("first_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_first_value_with_partition():
            expected_sql = """
                SELECT symbol, date, FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY date ASC) AS first_price FROM prices
            """

            q = source("prices").select(
                col("symbol"),
                col("date"),
                F.first_value(col("price"))
                .over(partition_by=[col("symbol")], order_by=[col("date").asc()])
                .alias("first_price"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_last_value():
        """Test LAST_VALUE window function."""

        def test_last_value():
            expected_sql = """
                SELECT date, LAST_VALUE(price) OVER (ORDER BY date ASC) AS last_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.last_value(col("price")).over(order_by=[col("date").asc()]).alias("last_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_last_value_with_partition():
            expected_sql = """
                SELECT symbol, date, LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY date ASC) AS last_price FROM prices
            """

            q = source("prices").select(
                col("symbol"),
                col("date"),
                F.last_value(col("price"))
                .over(partition_by=[col("symbol")], order_by=[col("date").asc()])
                .alias("last_price"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_aggregates_as_window_functions():
        """Test aggregate functions used as window functions."""

        def test_sum_over():
            expected_sql = """
                SELECT date, SUM(amount) OVER (ORDER BY date ASC) AS running_total FROM sales
            """

            q = source("sales").select(
                col("date"), F.sum(col("amount")).over(order_by=[col("date").asc()]).alias("running_total")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_sum_over_partition_by():
            expected_sql = """
                SELECT date, SUM(amount) OVER (PARTITION BY department) AS dept_total FROM sales
            """

            q = source("sales").select(
                col("date"),
                F.sum(col("amount")).over(partition_by=[col("department")]).alias("dept_total"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_over():
            expected_sql = """
                SELECT id, COUNT(*) OVER (PARTITION BY customer_id) AS customer_order_count FROM orders
            """

            q = source("orders").select(
                col("id"), F.count().over(partition_by=[col("customer_id")]).alias("customer_order_count")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_avg_over():
            expected_sql = """
                SELECT date, AVG(amount) OVER (PARTITION BY department ORDER BY date ASC) AS dept_avg FROM sales
            """

            q = source("sales").select(
                col("date"),
                F.avg(col("amount"))
                .over(partition_by=[col("department")], order_by=[col("date").asc()])
                .alias("dept_avg"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_min_over():
            expected_sql = """
                SELECT date, MIN(price) OVER (PARTITION BY symbol) AS min_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.min(col("price")).over(partition_by=[col("symbol")]).alias("min_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_max_over():
            expected_sql = """
                SELECT date, MAX(price) OVER (PARTITION BY symbol) AS max_price FROM prices
            """

            q = source("prices").select(
                col("date"), F.max(col("price")).over(partition_by=[col("symbol")]).alias("max_price")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_multiple_window_functions():
        """Test multiple window functions in single query."""

        def test_multiple_window_functions():
            expected_sql = """
                SELECT date, ROW_NUMBER() OVER (ORDER BY date ASC) AS row_num, SUM(amount) OVER (ORDER BY date ASC) AS running_total, AVG(amount) OVER (PARTITION BY department) AS dept_avg FROM sales
            """

            q = source("sales").select(
                col("date"),
                F.row_number().over(order_by=[col("date").asc()]).alias("row_num"),
                F.sum(col("amount")).over(order_by=[col("date").asc()]).alias("running_total"),
                F.avg(col("amount")).over(partition_by=[col("department")]).alias("dept_avg"),
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}
