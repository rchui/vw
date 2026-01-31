"""Integration tests for FILTER clause."""

from tests.utils import sql
from vw.postgres import F, col, param, render, source


def describe_filter_clause():
    """Test FILTER (WHERE ...) clause for aggregate functions."""

    def describe_filter_on_aggregates():
        """Test FILTER clause on aggregate functions."""

        def test_count_with_filter():
            """COUNT(*) FILTER (WHERE ...)."""
            q = source("orders").select(
                F.count().filter(col("status") == param("status", "completed")).alias("completed_orders")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT COUNT(*) FILTER (WHERE status = $status) AS completed_orders FROM orders"
            )
            assert result.params == {"status": "completed"}

        def test_count_column_with_filter():
            """COUNT(column) FILTER (WHERE ...)."""
            q = source("orders").select(
                F.count(col("id")).filter(col("status") == param("status", "pending")).alias("pending_orders")
            )
            result = render(q)
            assert result.query == sql("SELECT COUNT(id) FILTER (WHERE status = $status) AS pending_orders FROM orders")
            assert result.params == {"status": "pending"}

        def test_sum_with_filter():
            """SUM(column) FILTER (WHERE ...)."""
            q = source("orders").select(
                F.sum(col("amount")).filter(col("status") == param("status", "completed")).alias("completed_revenue")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT SUM(amount) FILTER (WHERE status = $status) AS completed_revenue FROM orders"
            )
            assert result.params == {"status": "completed"}

        def test_avg_with_filter():
            """AVG(column) FILTER (WHERE ...)."""
            q = source("products").select(
                F.avg(col("price"))
                .filter(col("category") == param("category", "electronics"))
                .alias("avg_electronics_price")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT AVG(price) FILTER (WHERE category = $category) AS avg_electronics_price FROM products"
            )
            assert result.params == {"category": "electronics"}

        def test_min_with_filter():
            """MIN(column) FILTER (WHERE ...)."""
            q = source("products").select(
                F.min(col("price")).filter(col("in_stock") == param("in_stock", True)).alias("min_available_price")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT MIN(price) FILTER (WHERE in_stock = $in_stock) AS min_available_price FROM products"
            )
            assert result.params == {"in_stock": True}

        def test_max_with_filter():
            """MAX(column) FILTER (WHERE ...)."""
            q = source("products").select(
                F.max(col("price")).filter(col("in_stock") == param("in_stock", True)).alias("max_available_price")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT MAX(price) FILTER (WHERE in_stock = $in_stock) AS max_available_price FROM products"
            )
            assert result.params == {"in_stock": True}

    def describe_filter_with_complex_conditions():
        """Test FILTER clause with complex conditions."""

        def test_filter_with_and():
            """FILTER with AND condition."""
            q = source("orders").select(
                F.count()
                .filter((col("status") == param("status", "completed")) & (col("amount") > param("min_amount", 100)))
                .alias("large_completed_orders")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT COUNT(*) FILTER (WHERE (status = $status) AND (amount > $min_amount)) AS large_completed_orders FROM orders"
            )
            assert result.params == {"status": "completed", "min_amount": 100}

        def test_filter_with_or():
            """FILTER with OR condition."""
            q = source("orders").select(
                F.sum(col("amount"))
                .filter(
                    (col("status") == param("status1", "completed")) | (col("status") == param("status2", "shipped"))
                )
                .alias("completed_or_shipped_revenue")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT SUM(amount) FILTER (WHERE (status = $status1) OR (status = $status2)) AS completed_or_shipped_revenue FROM orders"
            )
            assert result.params == {"status1": "completed", "status2": "shipped"}

        def test_filter_with_not():
            """FILTER with NOT condition."""
            q = source("orders").select(
                F.count().filter(~(col("status") == param("status", "cancelled"))).alias("non_cancelled_orders")
            )
            result = render(q)
            assert result.query == sql(
                "SELECT COUNT(*) FILTER (WHERE NOT (status = $status)) AS non_cancelled_orders FROM orders"
            )
            assert result.params == {"status": "cancelled"}

    def describe_filter_with_group_by():
        """Test FILTER clause with GROUP BY."""

        def test_filter_with_group_by():
            """FILTER with GROUP BY."""
            q = (
                source("orders")
                .select(
                    col("customer_id"),
                    F.count().filter(col("status") == param("status", "completed")).alias("completed_orders"),
                )
                .group_by(col("customer_id"))
            )
            result = render(q)
            assert result.query == sql(
                "SELECT customer_id, COUNT(*) FILTER (WHERE status = $status) AS completed_orders FROM orders GROUP BY customer_id"
            )
            assert result.params == {"status": "completed"}

        def test_multiple_filters_with_group_by():
            """Multiple FILTER clauses with GROUP BY."""
            q = (
                source("orders")
                .select(
                    col("customer_id"),
                    F.count().filter(col("status") == param("completed", "completed")).alias("completed_orders"),
                    F.count().filter(col("status") == param("pending", "pending")).alias("pending_orders"),
                    F.sum(col("amount"))
                    .filter(col("status") == param("completed_status", "completed"))
                    .alias("completed_revenue"),
                )
                .group_by(col("customer_id"))
            )
            result = render(q)
            assert result.query == sql(
                "SELECT customer_id, COUNT(*) FILTER (WHERE status = $completed) AS completed_orders, COUNT(*) FILTER (WHERE status = $pending) AS pending_orders, SUM(amount) FILTER (WHERE status = $completed_status) AS completed_revenue FROM orders GROUP BY customer_id"
            )
            assert result.params == {"completed": "completed", "pending": "pending", "completed_status": "completed"}

    def describe_filter_with_window_functions():
        """Test FILTER clause combined with window functions."""

        def test_filter_with_over():
            """FILTER combined with OVER clause."""
            q = source("orders").select(
                col("id"),
                F.count()
                .filter(col("status") == param("status", "completed"))
                .over(partition_by=[col("customer_id")])
                .alias("customer_completed_orders"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT id, COUNT(*) FILTER (WHERE status = $status) OVER (PARTITION BY customer_id) AS customer_completed_orders FROM orders"
            )
            assert result.params == {"status": "completed"}

        def test_filter_with_over_and_order_by():
            """FILTER with OVER and ORDER BY."""
            q = source("sales").select(
                col("date"),
                F.sum(col("amount"))
                .filter(col("category") == param("category", "electronics"))
                .over(order_by=[col("date").asc()])
                .alias("electronics_running_total"),
            )
            result = render(q)
            assert result.query == sql(
                "SELECT date, SUM(amount) FILTER (WHERE category = $category) OVER (ORDER BY date ASC) AS electronics_running_total FROM sales"
            )
            assert result.params == {"category": "electronics"}
