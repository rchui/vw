"""Integration tests for query building."""

from tests.utils import sql
from vw.postgres import col, ref, render


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

        q = ref("users").select(col("id"), col("name")).order_by(col("id")).limit(10, offset=20)
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
            ref("sales")
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

        q = ref("orders").select(col("user_id")).distinct()
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

        q = ref("events").select(col("event_type")).distinct().where(col("active")).order_by(col("event_type"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_distinct_on() -> None:
        expected_sql = """
        SELECT DISTINCT ON (department) id, name, department
        FROM employees
        ORDER BY department, salary DESC
        """

        q = (
            ref("employees")
            .select(col("id"), col("name"), col("department"))
            .distinct(col("department"))
            .order_by(col("department"), col("salary").desc())
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_distinct_on_multiple_cols() -> None:
        expected_sql = """
        SELECT DISTINCT ON (region, department) id, name
        FROM employees
        """

        q = ref("employees").select(col("id"), col("name")).distinct(col("region"), col("department"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_aliased_queries() -> None:
    def it_builds_query_with_table_alias() -> None:
        expected_sql = """
        SELECT u.id, u.name
        FROM users AS u
        """

        s = ref("users").alias("u")
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

        s = ref("orders").alias("o")
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

        s = ref("products").alias("p")
        q = s.select(s.star).where(s.col("active")).limit(10)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
