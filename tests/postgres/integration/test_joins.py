"""Integration tests for joins."""

from tests.utils import sql
from vw.postgres import F, col, param, render, source


def describe_inner_joins():
    def it_builds_basic_inner_join():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")]).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_inner_join_with_multiple_conditions():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id AND o.status = $status)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.inner(
            orders, on=[users.col("id") == orders.col("user_id"), orders.col("status") == param("status", "active")]
        ).select(users.col("id"), orders.col("total"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": "active"}

    def it_builds_multiple_inner_joins():
        expected_sql = """
        SELECT u.id, o.total, p.name
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        INNER JOIN products AS p ON (o.product_id = p.id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .join.inner(products, on=[orders.col("product_id") == products.col("id")])
            .select(users.col("id"), orders.col("total"), products.col("name"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_left_joins():
    def it_builds_basic_left_join():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        LEFT JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.left(orders, on=[users.col("id") == orders.col("user_id")]).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_left_join_with_where():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        LEFT JOIN orders AS o ON (u.id = o.user_id)
        WHERE u.active = $active
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("id"), orders.col("total"))
            .where(users.col("active") == param("active", True))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}


def describe_right_joins():
    def it_builds_basic_right_join():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        RIGHT JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.right(orders, on=[users.col("id") == orders.col("user_id")]).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_full_outer_joins():
    def it_builds_basic_full_outer_join():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        FULL JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")]).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_cross_joins():
    def it_builds_basic_cross_join():
        expected_sql = """
        SELECT u.id, t.tag
        FROM users AS u
        CROSS JOIN tags AS t
        """

        users = source("users").alias("u")
        tags = source("tags").alias("t")

        query = users.join.cross(tags).select(users.col("id"), tags.col("tag"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_cross_join_with_where():
        expected_sql = """
        SELECT u.id, t.tag
        FROM users AS u
        CROSS JOIN tags AS t
        WHERE u.active = $active
        """

        users = source("users").alias("u")
        tags = source("tags").alias("t")

        query = (
            users.join.cross(tags)
            .select(users.col("id"), tags.col("tag"))
            .where(users.col("active") == param("active", True))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}


def describe_using_clause():
    def it_builds_join_with_using_single_column():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o USING (user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.inner(orders, using=[col("user_id")]).select(users.col("id"), orders.col("total"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_join_with_using_multiple_columns():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o USING (user_id, tenant_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.inner(orders, using=[col("user_id"), col("tenant_id")]).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_joins_with_clauses():
    def it_builds_join_with_group_by():
        expected_sql = """
        SELECT u.name, COUNT(*) AS order_count
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        GROUP BY u.name
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("name"), F.count().alias("order_count"))
            .group_by(users.col("name"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_join_with_having():
        expected_sql = """
        SELECT u.name, SUM(o.total) AS total_spent
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        GROUP BY u.name
        HAVING SUM(o.total) > $min_total
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("name"), F.sum(orders.col("total")).alias("total_spent"))
            .group_by(users.col("name"))
            .having(F.sum(orders.col("total")) > param("min_total", 1000))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"min_total": 1000}

    def it_builds_join_with_order_by_and_limit():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        ORDER BY o.total DESC
        LIMIT 10
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("id"), orders.col("total"))
            .order_by(orders.col("total").desc())
            .limit(10)
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_join_with_distinct():
        expected_sql = """
        SELECT DISTINCT u.name
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")]).select(users.col("name")).distinct()
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_mixed_join_types():
    def it_builds_query_with_multiple_join_types():
        expected_sql = """
        SELECT u.id, o.total, p.name
        FROM users AS u
        LEFT JOIN orders AS o ON (u.id = o.user_id)
        INNER JOIN products AS p ON (o.product_id = p.id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        query = (
            users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
            .join.inner(products, on=[orders.col("product_id") == products.col("id")])
            .select(users.col("id"), orders.col("total"), products.col("name"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_joins_with_qualified_stars():
    def it_builds_join_with_qualified_stars():
        expected_sql = """
        SELECT u.*, o.*
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")]).select(users.star, orders.star)

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_self_joins():
    def it_builds_self_join():
        expected_sql = """
        SELECT e.name, m.name AS manager_name
        FROM employees AS e
        INNER JOIN employees AS m ON (e.manager_id = m.id)
        """

        employees = source("employees").alias("e")
        managers = source("employees").alias("m")

        query = employees.join.inner(managers, on=[employees.col("manager_id") == managers.col("id")]).select(
            employees.col("name"), managers.col("name").alias("manager_name")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}
