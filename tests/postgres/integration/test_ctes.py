"""Integration tests for Common Table Expressions (CTEs)."""

import pytest

from tests.utils import sql
from vw.core.exceptions import CTENameCollisionError
from vw.postgres import F, col, cte, param, render, source


def describe_basic_ctes():
    def it_renders_simple_cte():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT id, name FROM active_users
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(col("id"), col("name"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT id, name FROM active_users
        """)
        assert result.params == {"active": True}

    def it_renders_cte_with_where():
        """
        WITH high_value_orders AS (SELECT * FROM orders WHERE total > $min_total)
        SELECT * FROM high_value_orders WHERE status = $status
        """
        high_value_orders = cte(
            "high_value_orders",
            source("orders").select(col("*")).where(col("total") > param("min_total", 1000)),
        )

        query = high_value_orders.select(col("*")).where(col("status") == param("status", "completed"))

        result = render(query)
        assert result.query == sql("""
            WITH high_value_orders AS (SELECT * FROM orders WHERE total > $min_total)
            SELECT * FROM high_value_orders WHERE status = $status
        """)
        assert result.params == {"min_total": 1000, "status": "completed"}

    def it_renders_cte_with_qualified_columns():
        """
        WITH active_users AS (SELECT id, name, email FROM users WHERE active = $active)
        SELECT active_users.id, active_users.name FROM active_users
        """
        active_users = cte(
            "active_users",
            source("users").select(col("id"), col("name"), col("email")).where(col("active") == param("active", True)),
        )

        query = active_users.select(active_users.col("id"), active_users.col("name"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT id, name, email FROM users WHERE active = $active)
            SELECT active_users.id, active_users.name FROM active_users
        """)
        assert result.params == {"active": True}

    def it_renders_cte_with_parameters():
        """
        WITH filtered_users AS (SELECT * FROM users WHERE age >= $min_age AND country = $country)
        SELECT id, name FROM filtered_users
        """
        filtered_users = cte(
            "filtered_users",
            source("users")
            .select(col("*"))
            .where(col("age") >= param("min_age", 18))
            .where(col("country") == param("country", "US")),
        )

        query = filtered_users.select(col("id"), col("name"))

        result = render(query)
        assert result.query == sql("""
            WITH filtered_users AS (SELECT * FROM users WHERE age >= $min_age AND country = $country)
            SELECT id, name FROM filtered_users
        """)
        assert result.params == {"min_age": 18, "country": "US"}


def describe_cte_references():
    def it_uses_cte_in_from_clause():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT * FROM active_users
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(col("*"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT * FROM active_users
        """)
        assert result.params == {"active": True}

    def it_uses_cte_in_join():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT o.id, o.total, u.name
        FROM orders AS o
        INNER JOIN active_users AS u ON (o.user_id = u.id)
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        orders = source("orders").alias("o")
        users_cte = active_users.alias("u")

        query = orders.select(orders.col("id"), orders.col("total"), users_cte.col("name")).join.inner(
            users_cte, on=[orders.col("user_id") == users_cte.col("id")]
        )

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT o.id, o.total, u.name
            FROM orders AS o
            INNER JOIN active_users AS u ON (o.user_id = u.id)
        """)
        assert result.params == {"active": True}

    def it_uses_aliased_cte():
        """
        WITH users_summary AS (SELECT id, name FROM users)
        SELECT u.id, u.name FROM users_summary AS u
        """
        users_summary = cte("users_summary", source("users").select(col("id"), col("name")))

        query = users_summary.alias("u").select(col("u.id"), col("u.name"))

        result = render(query)
        assert result.query == sql("""
            WITH users_summary AS (SELECT id, name FROM users)
            SELECT u.id, u.name FROM users_summary AS u
        """)
        assert result.params == {}

    def it_qualifies_columns_with_cte_name():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT active_users.id FROM active_users
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(active_users.col("id"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT active_users.id FROM active_users
        """)
        assert result.params == {"active": True}

    def it_qualifies_columns_with_alias():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT u.id FROM active_users AS u
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )
        aliased_cte = active_users.alias("u")

        query = aliased_cte.select(aliased_cte.col("id"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT u.id FROM active_users AS u
        """)
        assert result.params == {"active": True}


def describe_multiple_ctes():
    def it_renders_two_ctes():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active),
        high_value_orders AS (SELECT * FROM orders WHERE total > $min_total)
        SELECT u.name, o.total
        FROM active_users AS u
        INNER JOIN high_value_orders AS o ON (u.id = o.user_id)
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        high_value_orders = cte(
            "high_value_orders",
            source("orders").select(col("*")).where(col("total") > param("min_total", 1000)),
        )

        u = active_users.alias("u")
        o = high_value_orders.alias("o")

        query = u.select(u.col("name"), o.col("total")).join.inner(o, on=[u.col("id") == o.col("user_id")])

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active),
            high_value_orders AS (SELECT * FROM orders WHERE total > $min_total)
            SELECT u.name, o.total
            FROM active_users AS u
            INNER JOIN high_value_orders AS o ON (u.id = o.user_id)
        """)
        assert result.params == {"active": True, "min_total": 1000}

    def it_renders_cte_referencing_cte():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active),
        premium_users AS (SELECT * FROM active_users WHERE premium = $premium)
        SELECT id, name FROM premium_users
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        premium_users = cte(
            "premium_users",
            active_users.select(col("*")).where(col("premium") == param("premium", True)),
        )

        query = premium_users.select(col("id"), col("name"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active),
            premium_users AS (SELECT * FROM active_users WHERE premium = $premium)
            SELECT id, name FROM premium_users
        """)
        assert result.params == {"active": True, "premium": True}


def describe_recursive_ctes():
    def it_renders_recursive_cte():
        """
        WITH RECURSIVE tree AS (SELECT * FROM items WHERE parent_id IS NULL)
        SELECT * FROM tree
        """
        tree = cte("tree", source("items").select(col("*")).where(col("parent_id").is_null()), recursive=True)

        query = tree.select(col("*"))

        result = render(query)
        assert result.query == sql("""
            WITH RECURSIVE tree AS (SELECT * FROM items WHERE parent_id IS NULL)
            SELECT * FROM tree
        """)
        assert result.params == {}

    def it_renders_recursive_with_union_all():
        """
        WITH RECURSIVE tree AS (
            (SELECT * FROM items WHERE parent_id IS NULL)
            UNION ALL
            (SELECT i.* FROM items AS i INNER JOIN tree AS t ON (i.parent_id = t.id))
        )
        SELECT * FROM tree
        """
        # Anchor: top-level items
        anchor = source("items").select(col("*")).where(col("parent_id").is_null())

        # Recursive part: join items with tree (self-reference via source)
        items = source("items").alias("i")
        recursive_part = items.select(items.star).join.inner(
            source("tree").alias("t"), on=[items.col("parent_id") == col("t.id")]
        )

        # Combine with UNION ALL
        combined = anchor + recursive_part

        # Create recursive CTE
        tree = cte("tree", combined, recursive=True)

        query = tree.select(col("*"))

        result = render(query)
        assert result.query == sql("""
            WITH RECURSIVE tree AS (
                (SELECT * FROM items WHERE parent_id IS NULL)
                UNION ALL
                (SELECT i.* FROM items AS i INNER JOIN tree AS t ON (i.parent_id = t.id))
            )
            SELECT * FROM tree
        """)
        assert result.params == {}

    def it_renders_mixed_recursive_and_non_recursive():
        """
        WITH RECURSIVE active_users AS (SELECT * FROM users WHERE active = $active),
        tree AS (SELECT * FROM items WHERE parent_id IS NULL)
        SELECT u.name, t.id
        FROM active_users AS u
        INNER JOIN tree AS t ON (u.id = t.user_id)
        """
        # Non-recursive CTE
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        # Recursive CTE
        tree = cte("tree", source("items").select(col("*")).where(col("parent_id").is_null()), recursive=True)

        u = active_users.alias("u")
        t = tree.alias("t")

        query = u.select(u.col("name"), t.col("id")).join.inner(t, on=[u.col("id") == t.col("user_id")])

        result = render(query)
        assert result.query == sql("""
            WITH RECURSIVE active_users AS (SELECT * FROM users WHERE active = $active),
            tree AS (SELECT * FROM items WHERE parent_id IS NULL)
            SELECT u.name, t.id
            FROM active_users AS u
            INNER JOIN tree AS t ON (u.id = t.user_id)
        """)
        assert result.params == {"active": True}


def describe_complex_scenarios():
    def it_combines_cte_with_group_by():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT status, COUNT(*) FROM active_users GROUP BY status
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(col("status"), F.count()).group_by(col("status"))

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT status, COUNT(*) FROM active_users GROUP BY status
        """)
        assert result.params == {"active": True}

    def it_combines_cte_with_order_by():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT id, name FROM active_users ORDER BY name ASC
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(col("id"), col("name")).order_by(col("name").asc())

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT id, name FROM active_users ORDER BY name ASC
        """)
        assert result.params == {"active": True}

    def it_combines_cte_with_limit():
        """
        WITH active_users AS (SELECT * FROM users WHERE active = $active)
        SELECT id, name FROM active_users LIMIT 10 OFFSET 5
        """
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        query = active_users.select(col("id"), col("name")).limit(10, offset=5)

        result = render(query)
        assert result.query == sql("""
            WITH active_users AS (SELECT * FROM users WHERE active = $active)
            SELECT id, name FROM active_users LIMIT 10 OFFSET 5
        """)
        assert result.params == {"active": True}

    def it_preserves_parameters_from_cte():
        """
        WITH filtered AS (SELECT * FROM users WHERE age >= $min_age AND country = $country)
        SELECT * FROM filtered WHERE premium = $premium
        """
        filtered = cte(
            "filtered",
            source("users")
            .select(col("*"))
            .where(col("age") >= param("min_age", 18))
            .where(col("country") == param("country", "US")),
        )

        query = filtered.select(col("*")).where(col("premium") == param("premium", True))

        result = render(query)
        assert result.query == sql("""
            WITH filtered AS (SELECT * FROM users WHERE age >= $min_age AND country = $country)
            SELECT * FROM filtered WHERE premium = $premium
        """)
        assert result.params == {"min_age": 18, "country": "US", "premium": True}


def describe_cte_name_collision():
    def it_handles_cte_name_collision():
        """Should raise CTENameCollisionError when same CTE name is used twice"""
        active_users = cte(
            "active_users", source("users").select(col("*")).where(col("active") == param("active", True))
        )

        # Try to reuse same CTE name - this should fail during rendering
        duplicate_users = cte(
            "active_users",
            source("users").select(col("*")).where(col("premium") == param("premium", True)),
        )

        # Join the two CTEs with the same name
        query = (
            active_users.alias("u1")
            .select(col("u1.id"))
            .join.inner(duplicate_users.alias("u2"), on=[col("u1.id") == col("u2.id")])
        )

        with pytest.raises(CTENameCollisionError) as error:
            render(query)
            assert "CTENameCollisionError" in str(error.value)
