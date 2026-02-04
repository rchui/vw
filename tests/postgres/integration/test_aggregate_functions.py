"""Integration tests for aggregate functions."""

from tests.utils import sql
from vw.postgres import F, col, param, render, source


def describe_aggregate_functions():
    """Test aggregate functions (COUNT, SUM, AVG, MIN, MAX)."""

    def describe_count():
        """Test COUNT function."""

        def test_count_star():
            expected_sql = """SELECT COUNT(*) FROM users"""

            q = source("users").select(F.count())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_column():
            expected_sql = """SELECT COUNT(id) FROM users"""

            q = source("users").select(F.count(col("id")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_distinct():
            expected_sql = """SELECT COUNT(DISTINCT email) FROM users"""

            q = source("users").select(F.count(col("email"), distinct=True))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_with_alias():
            expected_sql = """SELECT COUNT(*) AS total FROM users"""

            q = source("users").select(F.count().alias("total"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_sum():
        """Test SUM function."""

        def test_sum_column():
            expected_sql = """SELECT SUM(amount) FROM orders"""

            q = source("orders").select(F.sum(col("amount")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_sum_with_alias():
            expected_sql = """SELECT SUM(amount) AS total FROM orders"""

            q = source("orders").select(F.sum(col("amount")).alias("total"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_avg():
        """Test AVG function."""

        def test_avg_column():
            expected_sql = """SELECT AVG(price) FROM products"""

            q = source("products").select(F.avg(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_avg_with_alias():
            expected_sql = """SELECT AVG(price) AS avg_price FROM products"""

            q = source("products").select(F.avg(col("price")).alias("avg_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_min():
        """Test MIN function."""

        def test_min_column():
            expected_sql = """SELECT MIN(price) FROM products"""

            q = source("products").select(F.min(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_min_with_alias():
            expected_sql = """SELECT MIN(price) AS min_price FROM products"""

            q = source("products").select(F.min(col("price")).alias("min_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_max():
        """Test MAX function."""

        def test_max_column():
            expected_sql = """SELECT MAX(price) FROM products"""

            q = source("products").select(F.max(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_max_with_alias():
            expected_sql = """SELECT MAX(price) AS max_price FROM products"""

            q = source("products").select(F.max(col("price")).alias("max_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_with_group_by():
        """Test aggregates with GROUP BY."""

        def test_count_with_group_by():
            expected_sql = """SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"""

            q = source("orders").select(col("customer_id"), F.count()).group_by(col("customer_id"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_sum_with_group_by():
            expected_sql = """SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"""

            q = source("orders").select(col("customer_id"), F.sum(col("amount"))).group_by(col("customer_id"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_with_having():
        """Test aggregates in HAVING clause."""

        def test_having_count():
            expected_sql = """
                SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id HAVING COUNT(*) > $min_orders
            """

            q = (
                source("orders")
                .select(col("customer_id"), F.count().alias("order_count"))
                .group_by(col("customer_id"))
                .having(F.count() > param("min_orders", 5))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"min_orders": 5}

        def test_having_sum():
            expected_sql = """
                SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id HAVING SUM(amount) > $min_total
            """

            q = (
                source("orders")
                .select(col("customer_id"), F.sum(col("amount")).alias("total"))
                .group_by(col("customer_id"))
                .having(F.sum(col("amount")) > param("min_total", 1000))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"min_total": 1000}
