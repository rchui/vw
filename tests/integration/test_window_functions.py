"""Integration tests for window functions."""

import vw
from tests.utils import sql
from vw.functions import (
    avg,
    coalesce,
    count,
    dense_rank,
    first_value,
    greatest,
    lag,
    last_value,
    lead,
    least,
    max_,
    min_,
    ntile,
    nullif,
    rank,
    row_number,
    sum_,
)


def describe_window_only_functions():
    """Tests for functions that only make sense with OVER clause."""

    def it_generates_row_number_with_order_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, ROW_NUMBER() OVER (ORDER BY created_at DESC) FROM users
        """
        stmt = vw.Source(name="users").select(
            vw.col("id"),
            row_number().over(order_by=[vw.col("created_at").desc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_row_number_with_partition_and_order(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            row_number().over(
                partition_by=[vw.col("customer_id")],
                order_by=[vw.col("order_date").asc()],
            ),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_rank(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT name, RANK() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees
        """
        stmt = vw.Source(name="employees").select(
            vw.col("name"),
            rank().over(
                partition_by=[vw.col("department")],
                order_by=[vw.col("salary").desc()],
            ),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_dense_rank(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) FROM employees
        """
        stmt = vw.Source(name="employees").select(
            vw.col("name"),
            dense_rank().over(order_by=[vw.col("salary").desc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_ntile(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT student_id, NTILE(4) OVER (ORDER BY score DESC) FROM scores
        """
        stmt = vw.Source(name="scores").select(
            vw.col("student_id"),
            ntile(4).over(order_by=[vw.col("score").desc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_aggregate_functions_as_window():
    """Tests for aggregate functions used with OVER clause."""

    def it_generates_sum_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, SUM(amount) OVER (PARTITION BY customer_id) FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            sum_(vw.col("amount")).over(partition_by=[vw.col("customer_id")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_count_star_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, COUNT(*) OVER (PARTITION BY customer_id) FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            count().over(partition_by=[vw.col("customer_id")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_count_column_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, COUNT(discount) OVER (PARTITION BY customer_id) FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            count(vw.col("discount")).over(partition_by=[vw.col("customer_id")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_avg_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, AVG(price) OVER (PARTITION BY category) FROM products
        """
        stmt = vw.Source(name="products").select(
            vw.col("id"),
            avg(vw.col("price")).over(partition_by=[vw.col("category")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_min_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, MIN(price) OVER (PARTITION BY category) FROM products
        """
        stmt = vw.Source(name="products").select(
            vw.col("id"),
            min_(vw.col("price")).over(partition_by=[vw.col("category")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_max_as_window(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, MAX(price) OVER (PARTITION BY category) FROM products
        """
        stmt = vw.Source(name="products").select(
            vw.col("id"),
            max_(vw.col("price")).over(partition_by=[vw.col("category")]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_aggregate_functions_without_window():
    """Tests for aggregate functions used without OVER clause."""

    def it_generates_sum_as_aggregate(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT SUM(amount) FROM orders"
        stmt = vw.Source(name="orders").select(sum_(vw.col("amount")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_count_star(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COUNT(*) FROM users"
        stmt = vw.Source(name="users").select(count())
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_count_column(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COUNT(email) FROM users"
        stmt = vw.Source(name="users").select(count(vw.col("email")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_avg_as_aggregate(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT AVG(price) FROM products"
        stmt = vw.Source(name="products").select(avg(vw.col("price")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_min_as_aggregate(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT MIN(price) FROM products"
        stmt = vw.Source(name="products").select(min_(vw.col("price")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_max_as_aggregate(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT MAX(price) FROM products"
        stmt = vw.Source(name="products").select(max_(vw.col("price")))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_offset_functions():
    """Tests for LAG, LEAD, FIRST_VALUE, LAST_VALUE."""

    def it_generates_lag_with_default_offset(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LAG(price, 1) OVER (ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            lag(vw.col("price")).over(order_by=[vw.col("date").asc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_lag_with_custom_offset(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LAG(price, 3) OVER (ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            lag(vw.col("price"), 3).over(order_by=[vw.col("date").asc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_lag_with_default_value(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LAG(price, 1, 0) OVER (ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            lag(vw.col("price"), 1, vw.col("0")).over(order_by=[vw.col("date").asc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_lead_with_default_offset(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LEAD(price, 1) OVER (ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            lead(vw.col("price")).over(order_by=[vw.col("date").asc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_lead_with_custom_offset(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LEAD(price, 2) OVER (ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            lead(vw.col("price"), 2).over(order_by=[vw.col("date").asc()]),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_first_value(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, FIRST_VALUE(price) OVER (PARTITION BY product_id ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            first_value(vw.col("price")).over(
                partition_by=[vw.col("product_id")],
                order_by=[vw.col("date").asc()],
            ),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_last_value(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT date, LAST_VALUE(price) OVER (PARTITION BY product_id ORDER BY date ASC) FROM prices
        """
        stmt = vw.Source(name="prices").select(
            vw.col("date"),
            last_value(vw.col("price")).over(
                partition_by=[vw.col("product_id")],
                order_by=[vw.col("date").asc()],
            ),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_window_function_aliasing():
    """Tests for aliasing window functions."""

    def it_aliases_window_function(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, ROW_NUMBER() OVER (ORDER BY id ASC) AS row_num FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            row_number().over(order_by=[vw.col("id").asc()]).alias("row_num"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_aliases_aggregate_function(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT SUM(amount) AS total FROM orders"
        stmt = vw.Source(name="orders").select(
            sum_(vw.col("amount")).alias("total"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_aliases_window_aggregate(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, SUM(amount) OVER (PARTITION BY customer_id) AS running_total FROM orders
        """
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            sum_(vw.col("amount")).over(partition_by=[vw.col("customer_id")]).alias("running_total"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_empty_over_clause():
    """Tests for empty OVER() clause."""

    def it_generates_row_number_with_empty_over(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT id, ROW_NUMBER() OVER () FROM users"
        stmt = vw.Source(name="users").select(
            vw.col("id"),
            row_number().over(),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_sum_with_empty_over(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT id, SUM(amount) OVER () FROM orders"
        stmt = vw.Source(name="orders").select(
            vw.col("id"),
            sum_(vw.col("amount")).over(),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_multiple_window_functions():
    """Tests for multiple window functions in same query."""

    def it_generates_multiple_window_functions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id,
                ROW_NUMBER() OVER (ORDER BY amount DESC) AS rank,
                SUM(amount) OVER (PARTITION BY region) AS region_total,
                LAG(amount, 1) OVER (ORDER BY date ASC) AS prev_amount
            FROM sales
        """
        stmt = vw.Source(name="sales").select(
            vw.col("id"),
            row_number().over(order_by=[vw.col("amount").desc()]).alias("rank"),
            sum_(vw.col("amount")).over(partition_by=[vw.col("region")]).alias("region_total"),
            lag(vw.col("amount")).over(order_by=[vw.col("date").asc()]).alias("prev_amount"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_coalesce():
    """Tests for COALESCE function."""

    def it_generates_coalesce_with_two_args(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COALESCE(nickname, name) FROM users"
        stmt = vw.Source(name="users").select(
            coalesce(vw.col("nickname"), vw.col("name")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_coalesce_with_multiple_args(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COALESCE(preferred_email, work_email, personal_email) FROM users"
        stmt = vw.Source(name="users").select(
            coalesce(vw.col("preferred_email"), vw.col("work_email"), vw.col("personal_email")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_coalesce_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COALESCE(nickname, name) AS display_name FROM users"
        stmt = vw.Source(name="users").select(
            coalesce(vw.col("nickname"), vw.col("name")).alias("display_name"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_coalesce_with_param_default(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT COALESCE(nickname, :default_name) FROM users"
        stmt = vw.Source(name="users").select(
            coalesce(vw.col("nickname"), vw.param("default_name", "Unknown")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"default_name": "Unknown"})


def describe_nullif():
    """Tests for NULLIF function."""

    def it_generates_nullif_with_two_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT NULLIF(value, default_value) FROM settings"
        stmt = vw.Source(name="settings").select(
            nullif(vw.col("value"), vw.col("default_value")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_nullif_with_param(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT NULLIF(status, :empty) FROM users"
        stmt = vw.Source(name="users").select(
            nullif(vw.col("status"), vw.param("empty", "")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"empty": ""})

    def it_generates_nullif_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT NULLIF(divisor, :zero) AS safe_divisor FROM calc"
        stmt = vw.Source(name="calc").select(
            nullif(vw.col("divisor"), vw.param("zero", 0)).alias("safe_divisor"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"zero": 0})


def describe_greatest():
    """Tests for GREATEST function."""

    def it_generates_greatest_with_two_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT GREATEST(price, min_price) FROM products"
        stmt = vw.Source(name="products").select(
            greatest(vw.col("price"), vw.col("min_price")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_greatest_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT GREATEST(a, b, c) FROM values"
        stmt = vw.Source(name="values").select(
            greatest(vw.col("a"), vw.col("b"), vw.col("c")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_greatest_with_param(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT GREATEST(price, :floor) FROM products"
        stmt = vw.Source(name="products").select(
            greatest(vw.col("price"), vw.param("floor", 10)),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"floor": 10})

    def it_generates_greatest_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT GREATEST(price, :floor) AS final_price FROM products"
        stmt = vw.Source(name="products").select(
            greatest(vw.col("price"), vw.param("floor", 10)).alias("final_price"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"floor": 10})


def describe_least():
    """Tests for LEAST function."""

    def it_generates_least_with_two_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT LEAST(price, max_price) FROM products"
        stmt = vw.Source(name="products").select(
            least(vw.col("price"), vw.col("max_price")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_least_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT LEAST(a, b, c) FROM values"
        stmt = vw.Source(name="values").select(
            least(vw.col("a"), vw.col("b"), vw.col("c")),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_least_with_param(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT LEAST(price, :ceiling) FROM products"
        stmt = vw.Source(name="products").select(
            least(vw.col("price"), vw.param("ceiling", 100)),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"ceiling": 100})

    def it_generates_least_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT LEAST(price, :ceiling) AS capped_price FROM products"
        stmt = vw.Source(name="products").select(
            least(vw.col("price"), vw.param("ceiling", 100)).alias("capped_price"),
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"ceiling": 100})
