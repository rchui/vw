"""Integration tests for subquery SQL rendering."""

from tests.utils import sql
from vw.postgres import F, col, exists, param, render, source


def describe_exists():
    def it_builds_basic_exists():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (SELECT * FROM orders AS o WHERE o.user_id = u.id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(col("*")).where(
            exists(orders.select(col("*")).where(orders.col("user_id") == users.col("id")))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_not_exists():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE NOT (EXISTS (SELECT * FROM orders AS o WHERE o.user_id = u.id))
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(col("*")).where(
            ~exists(orders.select(col("*")).where(orders.col("user_id") == users.col("id")))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_exists_with_parameters():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (SELECT * FROM orders AS o WHERE o.user_id = u.id AND o.status = $status)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(col("*")).where(
            exists(
                orders.select(col("*"))
                .where(orders.col("user_id") == users.col("id"))
                .where(orders.col("status") == param("status", "active"))
            )
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": "active"}

    def it_builds_exists_with_complex_subquery():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (
        SELECT * FROM orders AS o
        WHERE o.user_id = u.id AND o.total > $min_total
        GROUP BY o.user_id
        HAVING COUNT(*) > $min_orders
        )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(col("*")).where(
            exists(
                orders.select(col("*"))
                .where(orders.col("user_id") == users.col("id"))
                .where(orders.col("total") > param("min_total", 100))
                .group_by(orders.col("user_id"))
                .having(F.count() > param("min_orders", 5))
            )
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"min_total": 100, "min_orders": 5}


def describe_in_subquery():
    def it_builds_in_with_subquery():
        expected_sql = """
        SELECT * FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE status = $status)
        """

        users = source("users")
        orders = source("orders")

        subquery = orders.select(col("user_id")).where(col("status") == param("status", "active"))

        query = users.select(col("*")).where(col("id").is_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": "active"}

    def it_builds_not_in_with_subquery():
        expected_sql = """
        SELECT * FROM users
        WHERE id NOT IN (SELECT user_id FROM banned_users)
        """

        users = source("users")
        banned_users = source("banned_users")

        subquery = banned_users.select(col("user_id"))

        query = users.select(col("*")).where(col("id").is_not_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_in_with_complex_subquery():
        expected_sql = """
        SELECT * FROM users
        WHERE id IN (
        SELECT o.user_id FROM orders AS o
        INNER JOIN products AS p ON (o.product_id = p.id)
        WHERE p.category = $category
        )
        """

        users = source("users")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        subquery = (
            orders.join.inner(products, on=[orders.col("product_id") == products.col("id")])
            .select(orders.col("user_id"))
            .where(products.col("category") == param("category", "electronics"))
        )

        query = users.select(col("*")).where(col("id").is_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"category": "electronics"}


def describe_correlated_subqueries():
    def it_builds_correlated_exists():
        expected_sql = """
            SELECT u.id, u.name FROM users AS u
            WHERE EXISTS (
            SELECT $one FROM orders AS o
            WHERE o.user_id = u.id AND o.status = $status
            )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(users.col("id"), users.col("name")).where(
            exists(
                orders.select(param("one", 1))
                .where(orders.col("user_id") == users.col("id"))
                .where(orders.col("status") == param("status", "completed"))
            )
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"one": 1, "status": "completed"}

    def it_builds_correlated_in():
        expected_sql = """
        SELECT * FROM products AS p
        WHERE p.category_id IN (
        SELECT c.id FROM categories AS c
        WHERE c.name = p.name
        )
        """

        products = source("products").alias("p")
        categories = source("categories").alias("c")

        subquery = categories.select(categories.col("id")).where(categories.col("name") == products.col("name"))

        query = products.select(col("*")).where(products.col("category_id").is_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_multiple_correlated_references():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (
        SELECT * FROM orders AS o
        WHERE o.user_id = u.id AND o.email = u.email
        )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = users.select(col("*")).where(
            exists(
                orders.select(col("*"))
                .where(orders.col("user_id") == users.col("id"))
                .where(orders.col("email") == users.col("email"))
            )
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_nested_subqueries():
    def it_builds_exists_in_subquery():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE u.id IN (
        SELECT o.user_id FROM orders AS o
        WHERE EXISTS (
        SELECT * FROM products AS p
        WHERE p.id = o.product_id AND p.available = $available
        )
        )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        innermost = (
            products.select(col("*"))
            .where(products.col("id") == orders.col("product_id"))
            .where(products.col("available") == param("available", True))
        )

        subquery = orders.select(orders.col("user_id")).where(exists(innermost))

        query = users.select(col("*")).where(users.col("id").is_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"available": True}

    def it_builds_in_with_in():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE u.id IN (
        SELECT o.user_id FROM orders AS o
        WHERE o.product_id IN (
        SELECT p.id FROM products AS p
        WHERE p.category = $category
        )
        )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        innermost = products.select(products.col("id")).where(
            products.col("category") == param("category", "electronics")
        )

        subquery = orders.select(orders.col("user_id")).where(orders.col("product_id").is_in(innermost))

        query = users.select(col("*")).where(users.col("id").is_in(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"category": "electronics"}


def describe_exists_with_joins():
    def it_builds_exists_with_inner_join():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (
        SELECT * FROM orders AS o
        INNER JOIN products AS p ON (o.product_id = p.id)
        WHERE o.user_id = u.id AND p.price > $min_price
        )
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        products = source("products").alias("p")

        subquery = (
            orders.select(col("*"))
            .join.inner(products, on=[orders.col("product_id") == products.col("id")])
            .where(orders.col("user_id") == users.col("id"))
            .where(products.col("price") > param("min_price", 100))
        )

        query = users.select(col("*")).where(exists(subquery))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"min_price": 100}


def describe_combined_conditions():
    def it_builds_exists_with_and():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE u.active = $active AND EXISTS (SELECT * FROM orders AS o WHERE o.user_id = u.id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")

        query = (
            users.select(col("*"))
            .where(users.col("active") == param("active", True))
            .where(exists(orders.select(col("*")).where(orders.col("user_id") == users.col("id"))))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}

    def it_builds_multiple_exists():
        expected_sql = """
        SELECT * FROM users AS u
        WHERE EXISTS (SELECT * FROM orders AS o WHERE o.user_id = u.id)
        AND EXISTS (SELECT * FROM reviews AS r WHERE r.user_id = u.id)
        """

        users = source("users").alias("u")
        orders = source("orders").alias("o")
        reviews = source("reviews").alias("r")

        query = (
            users.select(col("*"))
            .where(exists(orders.select(col("*")).where(orders.col("user_id") == users.col("id"))))
            .where(exists(reviews.select(col("*")).where(reviews.col("user_id") == users.col("id"))))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}
