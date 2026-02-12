"""Integration tests for aggregate functions."""

from tests.utils import sql
from vw.postgres import F, col, param, ref, render


def describe_aggregate_functions():
    """Test aggregate functions (COUNT, SUM, AVG, MIN, MAX)."""

    def describe_count():
        """Test COUNT function."""

        def test_count_star():
            expected_sql = """SELECT COUNT(*) FROM users"""

            q = ref("users").select(F.count())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_column():
            expected_sql = """SELECT COUNT(id) FROM users"""

            q = ref("users").select(F.count(col("id")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_distinct():
            expected_sql = """SELECT COUNT(DISTINCT email) FROM users"""

            q = ref("users").select(F.count(col("email"), distinct=True))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_count_with_alias():
            expected_sql = """SELECT COUNT(*) AS total FROM users"""

            q = ref("users").select(F.count().alias("total"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_sum():
        """Test SUM function."""

        def test_sum_column():
            expected_sql = """SELECT SUM(amount) FROM orders"""

            q = ref("orders").select(F.sum(col("amount")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_sum_with_alias():
            expected_sql = """SELECT SUM(amount) AS total FROM orders"""

            q = ref("orders").select(F.sum(col("amount")).alias("total"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_avg():
        """Test AVG function."""

        def test_avg_column():
            expected_sql = """SELECT AVG(price) FROM products"""

            q = ref("products").select(F.avg(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_avg_with_alias():
            expected_sql = """SELECT AVG(price) AS avg_price FROM products"""

            q = ref("products").select(F.avg(col("price")).alias("avg_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_min():
        """Test MIN function."""

        def test_min_column():
            expected_sql = """SELECT MIN(price) FROM products"""

            q = ref("products").select(F.min(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_min_with_alias():
            expected_sql = """SELECT MIN(price) AS min_price FROM products"""

            q = ref("products").select(F.min(col("price")).alias("min_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_max():
        """Test MAX function."""

        def test_max_column():
            expected_sql = """SELECT MAX(price) FROM products"""

            q = ref("products").select(F.max(col("price")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_max_with_alias():
            expected_sql = """SELECT MAX(price) AS max_price FROM products"""

            q = ref("products").select(F.max(col("price")).alias("max_price"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_with_group_by():
        """Test aggregates with GROUP BY."""

        def test_count_with_group_by():
            expected_sql = """SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"""

            q = ref("orders").select(col("customer_id"), F.count()).group_by(col("customer_id"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_sum_with_group_by():
            expected_sql = """SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"""

            q = ref("orders").select(col("customer_id"), F.sum(col("amount"))).group_by(col("customer_id"))
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
                ref("orders")
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
                ref("orders")
                .select(col("customer_id"), F.sum(col("amount")).alias("total"))
                .group_by(col("customer_id"))
                .having(F.sum(col("amount")) > param("min_total", 1000))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"min_total": 1000}

    def describe_bit_and():
        """Test BIT_AND function."""

        def test_bit_and_basic():
            expected_sql = """SELECT BIT_AND(flags) FROM user_roles"""

            q = ref("user_roles").select(F.bit_and(col("flags")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_and_with_alias():
            expected_sql = """SELECT BIT_AND(permissions) AS combined_perms FROM user_roles"""

            q = ref("user_roles").select(F.bit_and(col("permissions")).alias("combined_perms"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_and_with_group_by():
            expected_sql = """SELECT user_id, BIT_AND(permissions) AS user_perms FROM user_roles GROUP BY user_id"""

            q = (
                ref("user_roles")
                .select(col("user_id"), F.bit_and(col("permissions")).alias("user_perms"))
                .group_by(col("user_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_and_with_having():
            expected_sql = """
                SELECT user_id, BIT_AND(permissions) AS user_perms FROM user_roles GROUP BY user_id HAVING BIT_AND(permissions) > $min_perms
            """

            q = (
                ref("user_roles")
                .select(col("user_id"), F.bit_and(col("permissions")).alias("user_perms"))
                .group_by(col("user_id"))
                .having(F.bit_and(col("permissions")) > param("min_perms", 0))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"min_perms": 0}

        def test_bit_and_with_filter():
            expected_sql = """
                SELECT BIT_AND(permissions) FILTER (WHERE is_active = $active) AS active_perms FROM user_roles
            """

            q = ref("user_roles").select(
                F.bit_and(col("permissions")).filter(col("is_active") == param("active", True)).alias("active_perms")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"active": True}

    def describe_bit_or():
        """Test BIT_OR function."""

        def test_bit_or_basic():
            expected_sql = """SELECT BIT_OR(flags) FROM user_roles"""

            q = ref("user_roles").select(F.bit_or(col("flags")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_or_with_alias():
            expected_sql = """SELECT BIT_OR(permissions) AS combined_perms FROM user_roles"""

            q = ref("user_roles").select(F.bit_or(col("permissions")).alias("combined_perms"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_or_with_group_by():
            expected_sql = """SELECT user_id, BIT_OR(permissions) AS user_perms FROM user_roles GROUP BY user_id"""

            q = (
                ref("user_roles")
                .select(col("user_id"), F.bit_or(col("permissions")).alias("user_perms"))
                .group_by(col("user_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bit_or_with_having():
            expected_sql = """
                SELECT user_id, BIT_OR(permissions) AS user_perms FROM user_roles GROUP BY user_id HAVING BIT_OR(permissions) > $min_perms
            """

            q = (
                ref("user_roles")
                .select(col("user_id"), F.bit_or(col("permissions")).alias("user_perms"))
                .group_by(col("user_id"))
                .having(F.bit_or(col("permissions")) > param("min_perms", 0))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"min_perms": 0}

        def test_bit_or_with_filter():
            expected_sql = """
                SELECT BIT_OR(permissions) FILTER (WHERE is_active = $active) AS active_perms FROM user_roles
            """

            q = ref("user_roles").select(
                F.bit_or(col("permissions")).filter(col("is_active") == param("active", True)).alias("active_perms")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"active": True}

    def describe_bool_and():
        """Test BOOL_AND function."""

        def test_bool_and_basic():
            expected_sql = """SELECT BOOL_AND(is_active) FROM users"""

            q = ref("users").select(F.bool_and(col("is_active")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_and_with_alias():
            expected_sql = """SELECT BOOL_AND(verified) AS all_verified FROM users"""

            q = ref("users").select(F.bool_and(col("verified")).alias("all_verified"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_and_with_group_by():
            expected_sql = """SELECT group_id, BOOL_AND(is_active) AS all_active FROM users GROUP BY group_id"""

            q = (
                ref("users")
                .select(col("group_id"), F.bool_and(col("is_active")).alias("all_active"))
                .group_by(col("group_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_and_with_having():
            expected_sql = """
                SELECT group_id, BOOL_AND(is_active) AS all_active FROM users GROUP BY group_id HAVING BOOL_AND(is_active) = $expected
            """

            q = (
                ref("users")
                .select(col("group_id"), F.bool_and(col("is_active")).alias("all_active"))
                .group_by(col("group_id"))
                .having(F.bool_and(col("is_active")) == param("expected", True))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"expected": True}

        def test_bool_and_with_filter():
            expected_sql = """
                SELECT BOOL_AND(is_active) FILTER (WHERE role = $role) AS admins_active FROM users
            """

            q = ref("users").select(
                F.bool_and(col("is_active")).filter(col("role") == param("role", "admin")).alias("admins_active")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"role": "admin"}

    def describe_bool_or():
        """Test BOOL_OR function."""

        def test_bool_or_basic():
            expected_sql = """SELECT BOOL_OR(needs_review) FROM orders"""

            q = ref("orders").select(F.bool_or(col("needs_review")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_or_with_alias():
            expected_sql = """SELECT BOOL_OR(has_error) AS any_errors FROM orders"""

            q = ref("orders").select(F.bool_or(col("has_error")).alias("any_errors"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_or_with_group_by():
            expected_sql = """SELECT customer_id, BOOL_OR(is_paid) AS any_paid FROM orders GROUP BY customer_id"""

            q = (
                ref("orders")
                .select(col("customer_id"), F.bool_or(col("is_paid")).alias("any_paid"))
                .group_by(col("customer_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def test_bool_or_with_having():
            expected_sql = """
                SELECT customer_id, BOOL_OR(is_paid) AS any_paid FROM orders GROUP BY customer_id HAVING BOOL_OR(is_paid) = $expected
            """

            q = (
                ref("orders")
                .select(col("customer_id"), F.bool_or(col("is_paid")).alias("any_paid"))
                .group_by(col("customer_id"))
                .having(F.bool_or(col("is_paid")) == param("expected", True))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"expected": True}

        def test_bool_or_with_filter():
            expected_sql = """
                SELECT BOOL_OR(needs_review) FILTER (WHERE priority = $priority) AS high_priority_reviews FROM orders
            """

            q = ref("orders").select(
                F.bool_or(col("needs_review"))
                .filter(col("priority") == param("priority", "high"))
                .alias("high_priority_reviews")
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"priority": "high"}
