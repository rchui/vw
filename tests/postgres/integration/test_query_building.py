"""Integration tests for query building."""

from tests.utils import sql
from vw.postgres import col, render, source


def describe_basic_queries() -> None:
    def it_builds_simple_select() -> None:
        expected_sql = """
            SELECT id, name, email
            FROM users
        """

        q = source("users").select(col("id"), col("name"), col("email"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_select_with_where() -> None:
        expected_sql = """
        SELECT id, name
        FROM users
        WHERE active
        """

        q = source("users").select(col("id"), col("name")).where(col("active"))
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

        q = source("users").select(col("id"), col("name")).order_by(col("id")).limit(10, offset=20)
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

        q = source("users").select(col("id"), col("name")).where(col("active"), col("verified"), col("premium"))
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
            source("products")
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

        q = source("orders").select(col("user_id"), col("total")).group_by(col("user_id"))
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

        q = source("orders").select(col("user_id"), col("total")).group_by(col("user_id")).having(col("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_complex_aggregation() -> None:
        expected_sql = """
        SELECT product_id, category, revenue
        FROM sales
        WHERE year AND region
        GROUP BY product_id, category
        HAVING revenue
        ORDER BY revenue
        LIMIT 10
        """

        q = (
            source("sales")
            .select(col("product_id"), col("category"), col("revenue"))
            .where(col("year"), col("region"))
            .group_by(col("product_id"), col("category"))
            .having(col("revenue"))
            .order_by(col("revenue"))
            .limit(10)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_distinct_queries() -> None:
    def it_builds_distinct_select() -> None:
        expected_sql = """
        SELECT DISTINCT user_id
        FROM orders
        """

        q = source("orders").select(col("user_id")).distinct()
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_distinct_with_filters() -> None:
        expected_sql = """
        SELECT DISTINCT event_type
        FROM events
        WHERE active
        ORDER BY event_type
        """

        q = source("events").select(col("event_type")).distinct().where(col("active")).order_by(col("event_type"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_aliased_queries() -> None:
    def it_builds_query_with_table_alias() -> None:
        expected_sql = """
        SELECT u.id, u.name
        FROM users AS u
        """

        s = source("users").alias("u")
        q = s.select(s.col("id"), s.col("name"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_query_with_qualified_columns() -> None:
        expected_sql = """
        SELECT o.id, o.user_id, o.total
        FROM orders AS o
        WHERE o.status
        ORDER BY o.created_at
        LIMIT 100
        """

        s = source("orders").alias("o")
        q = (
            s.select(s.col("id"), s.col("user_id"), s.col("total"))
            .where(s.col("status"))
            .order_by(s.col("created_at"))
            .limit(100)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_query_with_star() -> None:
        expected_sql = """
        SELECT p.*
        FROM products AS p
        WHERE p.active
        LIMIT 10
        """

        s = source("products").alias("p")
        q = s.select(s.star).where(s.col("active")).limit(10)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_real_world_patterns() -> None:
    def it_builds_user_search_query() -> None:
        expected_sql = """
        SELECT u.id, u.email, u.name, u.created_at
        FROM users AS u
        WHERE u.active AND u.verified
        ORDER BY u.created_at
        LIMIT 25
        """

        s = source("users").alias("u")
        q = (
            s.select(s.col("id"), s.col("email"), s.col("name"), s.col("created_at"))
            .where(s.col("active"), s.col("verified"))
            .order_by(s.col("created_at"))
            .limit(25)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_top_sellers_report() -> None:
        expected_sql = """
        SELECT oi.product_id, oi.quantity_sold, oi.revenue
        FROM order_items AS oi
        WHERE oi.year
        GROUP BY oi.product_id
        HAVING oi.quantity_sold
        ORDER BY oi.revenue
        LIMIT 20
        """

        s = source("order_items").alias("oi")
        q = (
            s.select(s.col("product_id"), s.col("quantity_sold"), s.col("revenue"))
            .where(s.col("year"))
            .group_by(s.col("product_id"))
            .having(s.col("quantity_sold"))
            .order_by(s.col("revenue"))
            .limit(20)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_distinct_categories_query() -> None:
        expected_sql = """
        SELECT DISTINCT p.category
        FROM products AS p
        WHERE p.published
        ORDER BY p.category
        """

        s = source("products").alias("p")
        q = s.select(s.col("category")).distinct().where(s.col("published")).order_by(s.col("category"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
