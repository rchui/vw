"""Integration tests for operators and expressions."""

from tests.utils import sql
from vw.postgres import col, param, render, source


def describe_comparison_operators() -> None:
    def it_renders_equals() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE age = $age
        """

        q = source("users").select(col("id")).where(col("age") == param("age", 18))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"age": 18}

    def it_renders_not_equals() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE status <> $status
        """

        q = source("users").select(col("id")).where(col("status") != param("status", "inactive"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": "inactive"}

    def it_renders_less_than() -> None:
        expected_sql = """
            SELECT id
            FROM products
            WHERE price < $price
        """

        q = source("products").select(col("id")).where(col("price") < param("price", 100))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"price": 100}

    def it_renders_less_than_or_equal() -> None:
        expected_sql = """
            SELECT id
            FROM products
            WHERE price <= $price
        """

        q = source("products").select(col("id")).where(col("price") <= param("price", 50))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"price": 50}

    def it_renders_greater_than() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE age > $age
        """

        q = source("users").select(col("id")).where(col("age") > param("age", 21))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"age": 21}

    def it_renders_greater_than_or_equal() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE age >= $age
        """

        q = source("users").select(col("id")).where(col("age") >= param("age", 18))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"age": 18}

    def it_compares_two_columns() -> None:
        expected_sql = """
        SELECT id
        FROM orders
        WHERE paid_amount >= total_amount
        """

        q = source("orders").select(col("id")).where(col("paid_amount") >= col("total_amount"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_arithmetic_operators() -> None:
    def it_renders_addition() -> None:
        expected_sql = """
        SELECT price + tax
        FROM orders
        """

        q = source("orders").select(col("price") + col("tax"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_subtraction() -> None:
        expected_sql = """
        SELECT total - discount
        FROM invoices
        """

        q = source("invoices").select(col("total") - col("discount"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_multiplication() -> None:
        expected_sql = """
        SELECT price * quantity
        FROM order_items
        """

        q = source("order_items").select(col("price") * col("quantity"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_division() -> None:
        expected_sql = """
        SELECT total / count
        FROM aggregates
        """

        q = source("aggregates").select(col("total") / col("count"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_modulo() -> None:
        expected_sql = """
            SELECT id % $divisor
            FROM items
        """

        q = source("items").select(col("id") % param("divisor", 10))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"divisor": 10}

    def it_renders_complex_arithmetic() -> None:
        expected_sql = """
        SELECT price * quantity + tax
        FROM orders
        """

        q = source("orders").select(col("price") * col("quantity") + col("tax"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_logical_operators() -> None:
    def it_renders_and() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE (active) AND (verified)
        """

        q = source("users").select(col("id")).where(col("active") & col("verified"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_or() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE (role = $admin) OR (role = $mod)
        """

        q = (
            source("users")
            .select(col("id"))
            .where((col("role") == param("admin", "admin")) | (col("role") == param("mod", "moderator")))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"admin": "admin", "mod": "moderator"}

    def it_renders_not() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE NOT (deleted)
        """

        q = source("users").select(col("id")).where(~col("deleted"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_complex_logic() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE ((active) AND (verified)) OR (role = $role)
        """

        q = (
            source("users")
            .select(col("id"))
            .where((col("active") & col("verified")) | (col("role") == param("role", "admin")))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"role": "admin"}


def describe_pattern_matching() -> None:
    def it_renders_like() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE email LIKE $pattern
        """

        q = source("users").select(col("id")).where(col("email").like(param("pattern", "%@example.com")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"pattern": "%@example.com"}

    def it_renders_not_like() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE name NOT LIKE $pattern
        """

        q = source("users").select(col("id")).where(col("name").not_like(param("pattern", "test%")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"pattern": "test%"}

    def it_renders_in_with_params() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE status IN ($s1, $s2, $s3)
        """

        q = (
            source("users")
            .select(col("id"))
            .where(col("status").is_in(param("s1", "active"), param("s2", "pending"), param("s3", "verified")))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"s1": "active", "s2": "pending", "s3": "verified"}

    def it_renders_not_in() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE role NOT IN ($r1, $r2)
        """

        q = (
            source("users")
            .select(col("id"))
            .where(col("role").is_not_in(param("r1", "banned"), param("r2", "suspended")))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"r1": "banned", "r2": "suspended"}

    def it_renders_between() -> None:
        expected_sql = """
            SELECT id
            FROM products
            WHERE price BETWEEN $low AND $high
        """

        q = source("products").select(col("id")).where(col("price").between(param("low", 10), param("high", 100)))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"low": 10, "high": 100}

    def it_renders_not_between() -> None:
        expected_sql = """
            SELECT id
            FROM products
            WHERE price NOT BETWEEN $low AND $high
        """

        q = source("products").select(col("id")).where(col("price").not_between(param("low", 50), param("high", 200)))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"low": 50, "high": 200}


def describe_null_checks() -> None:
    def it_renders_is_null() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE deleted_at IS NULL
        """

        q = source("users").select(col("id")).where(col("deleted_at").is_null())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_is_not_null() -> None:
        expected_sql = """
        SELECT id
        FROM users
        WHERE email IS NOT NULL
        """

        q = source("users").select(col("id")).where(col("email").is_not_null())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_expression_modifiers() -> None:
    def it_renders_alias() -> None:
        expected_sql = """
        SELECT price * quantity AS total
        FROM order_items
        """

        q = source("order_items").select((col("price") * col("quantity")).alias("total"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_cast() -> None:
        expected_sql = """
        SELECT id::text
        FROM users
        """

        q = source("users").select(col("id").cast("text"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_asc() -> None:
        expected_sql = """
        SELECT id
        FROM users
        ORDER BY created_at ASC
        """

        q = source("users").select(col("id")).order_by(col("created_at").asc())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_desc() -> None:
        expected_sql = """
        SELECT id
        FROM users
        ORDER BY created_at DESC
        """

        q = source("users").select(col("id")).order_by(col("created_at").desc())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_parameter_types() -> None:
    def it_renders_null_param() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE email = $email
        """

        q = source("users").select(col("id")).where(col("email") == param("email", None))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"email": None}

    def it_renders_boolean_true() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE active = $active
        """

        q = source("users").select(col("id")).where(col("active") == param("active", True))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}

    def it_renders_boolean_false() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE deleted = $deleted
        """

        q = source("users").select(col("id")).where(col("deleted") == param("deleted", False))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"deleted": False}

    def it_renders_integer() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE age = $age
        """

        q = source("users").select(col("id")).where(col("age") == param("age", 25))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"age": 25}

    def it_renders_float() -> None:
        expected_sql = """
            SELECT id
            FROM products
            WHERE price = $price
        """

        q = source("products").select(col("id")).where(col("price") == param("price", 19.99))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"price": 19.99}

    def it_renders_string_with_quotes() -> None:
        expected_sql = """
            SELECT id
            FROM users
            WHERE name = $name
        """

        q = source("users").select(col("id")).where(col("name") == param("name", "O'Brien"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"name": "O'Brien"}


def describe_real_world_patterns() -> None:
    def it_builds_filtered_price_query() -> None:
        expected_sql = """
            SELECT id, name, price * $tax_multiplier AS price_with_tax
            FROM products
            WHERE (((price >= $min) AND (price <= $max)) AND (in_stock)) AND (category = $cat)
            ORDER BY price ASC
            LIMIT 20
        """

        q = (
            source("products")
            .select(col("id"), col("name"), (col("price") * param("tax_multiplier", 1.1)).alias("price_with_tax"))
            .where(
                (col("price") >= param("min", 10))
                & (col("price") <= param("max", 100))
                & col("in_stock")
                & (col("category") == param("cat", "electronics"))
            )
            .order_by(col("price").asc())
            .limit(20)
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"tax_multiplier": 1.1, "min": 10, "max": 100, "cat": "electronics"}

    def it_builds_user_search_with_nulls() -> None:
        expected_sql = """
            SELECT id, email, name
            FROM users
            WHERE ((email IS NOT NULL) AND (email LIKE $domain)) OR (role = $role)
            ORDER BY created_at DESC
        """

        q = (
            source("users")
            .select(col("id"), col("email"), col("name"))
            .where(
                (col("email").is_not_null() & col("email").like(param("domain", "%@company.com")))
                | (col("role") == param("role", "admin"))
            )
            .order_by(col("created_at").desc())
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"domain": "%@company.com", "role": "admin"}
