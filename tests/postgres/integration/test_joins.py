"""Integration tests for joins."""

from tests.utils import sql
from vw.postgres import F, col, param, ref, render


def describe_inner_joins():
    def it_builds_basic_inner_join():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        products = ref("products").alias("p")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        tags = ref("tags").alias("t")

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

        users = ref("users").alias("u")
        tags = ref("tags").alias("t")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        products = ref("products").alias("p")

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

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

        query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")]).select(
            users.star(), orders.star()
        )

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

        employees = ref("employees").alias("e")
        managers = ref("employees").alias("m")

        query = employees.join.inner(managers, on=[employees.col("manager_id") == managers.col("id")]).select(
            employees.col("name"), managers.col("name").alias("manager_name")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_lateral_joins():
    def it_builds_inner_join_lateral_basic():
        expected_sql = """
        SELECT u.id, o.total
        FROM users AS u
        INNER JOIN LATERAL orders AS o ON (u.id = o.user_id)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")

        query = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")], lateral=True).select(
            users.col("id"), orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_left_join_lateral():
        expected_sql = """
        SELECT u.id, recent.created_at
        FROM users AS u
        LEFT JOIN LATERAL (
            SELECT created_at
            FROM orders
            WHERE user_id = u.id
            ORDER BY created_at DESC
            LIMIT 3
        ) AS recent ON (TRUE)
        """

        users = ref("users").alias("u")
        orders = ref("orders")

        recent_orders = (
            orders.select(orders.col("created_at"))
            .where(orders.col("user_id") == users.col("id"))
            .order_by(orders.col("created_at").desc())
            .limit(3)
            .alias("recent")
        )

        query = users.join.left(recent_orders, on=[col("TRUE")], lateral=True).select(
            users.col("id"), recent_orders.col("created_at")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_cross_join_lateral_with_function():
        expected_sql = """
        SELECT u.id, n
        FROM users AS u
        CROSS JOIN LATERAL generate_series(1, 5) AS n
        """

        users = ref("users").alias("u")
        series = ref("generate_series(1, 5)").alias("n")

        query = users.join.cross(series, lateral=True).select(users.col("id"), col("n"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_lateral_with_using_clause():
        expected_sql = """
        SELECT id, value
        FROM t1
        INNER JOIN LATERAL t2 USING (id)
        """

        t1 = ref("t1")
        t2 = ref("t2")

        query = t1.join.inner(t2, using=[col("id")], lateral=True).select(col("id"), col("value"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_multiple_lateral_joins():
        expected_sql = """
        SELECT id, value, value
        FROM t1
        LEFT JOIN LATERAL t2 ON (id = t1_id)
        LEFT JOIN LATERAL t3 ON (id = t2_id)
        """

        t1 = ref("t1")
        t2 = ref("t2")
        t3 = ref("t3")

        query = (
            t1.join.left(t2, on=[col("id") == col("t1_id")], lateral=True)
            .join.left(t3, on=[col("id") == col("t2_id")], lateral=True)
            .select(col("id"), col("value"), col("value"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_lateral_with_parameters():
        expected_sql = """
        SELECT u.id, recent.total
        FROM users AS u
        LEFT JOIN LATERAL (
            SELECT total
            FROM orders
            WHERE (user_id = u.id) AND (status = $status)
            LIMIT 5
        ) AS recent ON (TRUE)
        """

        users = ref("users").alias("u")
        orders = ref("orders")

        recent_orders = (
            orders.select(orders.col("total"))
            .where((orders.col("user_id") == users.col("id")) & (orders.col("status") == param("status", "active")))
            .limit(5)
            .alias("recent")
        )

        query = users.join.left(recent_orders, on=[col("TRUE")], lateral=True).select(
            users.col("id"), recent_orders.col("total")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": "active"}

    def it_builds_right_join_lateral():
        expected_sql = """
        SELECT id, value
        FROM t1
        RIGHT JOIN LATERAL t2 ON (id = t1_id)
        """

        t1 = ref("t1")
        t2 = ref("t2")

        query = t1.join.right(t2, on=[col("id") == col("t1_id")], lateral=True).select(col("id"), col("value"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_full_outer_join_lateral():
        expected_sql = """
        SELECT id, value
        FROM t1
        FULL JOIN LATERAL t2 ON (id = t1_id)
        """

        t1 = ref("t1")
        t2 = ref("t2")

        query = t1.join.full_outer(t2, on=[col("id") == col("t1_id")], lateral=True).select(col("id"), col("value"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_lateral_with_aggregate():
        expected_sql = """
        SELECT u.name, recent_stats.total_amount
        FROM users AS u
        LEFT JOIN LATERAL (
            SELECT SUM(total) AS total_amount
            FROM orders
            WHERE user_id = u.id
        ) AS recent_stats ON (TRUE)
        """

        users = ref("users").alias("u")
        orders = ref("orders")

        recent_stats = (
            orders.select(F.sum(orders.col("total")).alias("total_amount"))
            .where(orders.col("user_id") == users.col("id"))
            .alias("recent_stats")
        )

        query = users.join.left(recent_stats, on=[col("TRUE")], lateral=True).select(
            users.col("name"), recent_stats.col("total_amount")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_mixed_lateral_and_regular_joins():
        expected_sql = """
        SELECT u.id, o.total, recent.product_id
        FROM users AS u
        INNER JOIN orders AS o ON (u.id = o.user_id)
        LEFT JOIN LATERAL (
            SELECT product_id
            FROM order_items
            WHERE order_id = o.id
            LIMIT 1
        ) AS recent ON (TRUE)
        """

        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        order_items = ref("order_items")

        recent_item = (
            order_items.select(order_items.col("product_id"))
            .where(order_items.col("order_id") == orders.col("id"))
            .limit(1)
            .alias("recent")
        )

        query = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .join.left(recent_item, on=[col("TRUE")], lateral=True)
            .select(users.col("id"), orders.col("total"), recent_item.col("product_id"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_lateral_without_on_clause():
        expected_sql = """
        SELECT u.id, n
        FROM users AS u
        CROSS JOIN LATERAL generate_series(1, u.id) AS n
        """

        users = ref("users").alias("u")
        series = ref("generate_series(1, u.id)").alias("n")

        query = users.join.cross(series, lateral=True).select(users.col("id"), col("n"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_lateral_join_with_where_clause():
        expected_sql = """
        SELECT u.id, recent.total
        FROM users AS u
        LEFT JOIN LATERAL (
            SELECT total
            FROM orders
            WHERE user_id = u.id
        ) AS recent ON (TRUE)
        WHERE u.active = $active
        """

        users = ref("users").alias("u")
        orders = ref("orders")

        recent_orders = (
            orders.select(orders.col("total")).where(orders.col("user_id") == users.col("id")).alias("recent")
        )

        query = (
            users.join.left(recent_orders, on=[col("TRUE")], lateral=True)
            .select(users.col("id"), recent_orders.col("total"))
            .where(users.col("active") == param("active", True))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}
