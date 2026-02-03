"""Integration tests for set operation SQL rendering."""

from tests.utils import sql
from vw.postgres import col, param, render, source


def describe_union():
    def it_builds_basic_union():
        """
        (SELECT id FROM users) UNION (SELECT id FROM admins)
        """
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        result = render(query1 | query2)

        assert result.query == sql("""
            (SELECT id FROM users) UNION (SELECT id FROM admins)
        """)
        assert result.params == {}

    def it_builds_union_all():
        """
        (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """
        query1 = source("users").select(col("id"))
        query2 = source("admins").select(col("id"))

        result = render(query1 + query2)

        assert result.query == sql("""
            (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """)
        assert result.params == {}

    def it_builds_union_with_where():
        """
        (SELECT id FROM users WHERE active = $active) UNION (SELECT id FROM admins)
        """
        query1 = source("users").select(col("id")).where(col("active") == param("active", True))
        query2 = source("admins").select(col("id"))

        result = render(query1 | query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE active = $active) UNION (SELECT id FROM admins)
        """)
        assert result.params == {"active": True}


def describe_intersect():
    def it_builds_basic_intersect():
        """
        (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
        """
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        result = render(query1 & query2)

        assert result.query == sql("""
            (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
        """)
        assert result.params == {}

    def it_builds_intersect_with_conditions():
        """
        (SELECT id FROM users WHERE age >= $min_age)
        INTERSECT
        (SELECT user_id FROM banned WHERE reason = $reason)
        """
        query1 = source("users").select(col("id")).where(col("age") >= param("min_age", 18))
        query2 = source("banned").select(col("user_id")).where(col("reason") == param("reason", "spam"))

        result = render(query1 & query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE age >= $min_age)
            INTERSECT
            (SELECT user_id FROM banned WHERE reason = $reason)
        """)
        assert result.params == {"min_age": 18, "reason": "spam"}


def describe_except():
    def it_builds_basic_except():
        """
        (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
        """
        query1 = source("users").select(col("id"))
        query2 = source("banned").select(col("user_id"))

        result = render(query1 - query2)

        assert result.query == sql("""
            (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
        """)
        assert result.params == {}

    def it_builds_except_with_conditions():
        """
        (SELECT id FROM users WHERE active = $active)
        EXCEPT
        (SELECT user_id FROM banned)
        """
        query1 = source("users").select(col("id")).where(col("active") == param("active", True))
        query2 = source("banned").select(col("user_id"))

        result = render(query1 - query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE active = $active)
            EXCEPT
            (SELECT user_id FROM banned)
        """)
        assert result.params == {"active": True}


def describe_chaining():
    def it_chains_multiple_unions():
        """
        ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)
        """
        users = source("users").select(col("id"))
        admins = source("admins").select(col("id"))
        guests = source("guests").select(col("id"))

        result = render((users | admins) | guests)

        assert result.query == sql("""
            ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)
        """)

    def it_chains_mixed_operations():
        """
        ((SELECT id FROM users) UNION (SELECT id FROM admins))
        EXCEPT
        (SELECT user_id FROM banned)
        """
        users = source("users").select(col("id"))
        admins = source("admins").select(col("id"))
        banned = source("banned").select(col("user_id"))

        result = render((users | admins) - banned)

        assert result.query == sql("""
            ((SELECT id FROM users) UNION (SELECT id FROM admins))
            EXCEPT
            (SELECT user_id FROM banned)
        """)

    def it_handles_complex_nesting():
        """
        (SELECT id FROM users)
        UNION
        ((SELECT id FROM admins) EXCEPT (SELECT user_id FROM banned))
        """
        users = source("users").select(col("id"))
        admins = source("admins").select(col("id"))
        banned = source("banned").select(col("user_id"))

        result = render(users | (admins - banned))

        assert result.query == sql("""
            (SELECT id FROM users)
            UNION
            ((SELECT id FROM admins) EXCEPT (SELECT user_id FROM banned))
        """)


def describe_parameters():
    def it_preserves_parameters_across_union():
        """
        (SELECT id FROM users WHERE active = $active)
        UNION
        (SELECT id FROM admins WHERE role = $role)
        """
        query1 = source("users").select(col("id")).where(col("active") == param("active", True))
        query2 = source("admins").select(col("id")).where(col("role") == param("role", "admin"))

        result = render(query1 | query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE active = $active)
            UNION
            (SELECT id FROM admins WHERE role = $role)
        """)
        assert result.params == {"active": True, "role": "admin"}

    def it_merges_parameters_from_both_sides():
        """
        (SELECT id FROM users WHERE age >= $min_age AND active = $active)
        INTERSECT
        (SELECT user_id FROM premium WHERE tier = $tier)
        """
        query1 = (
            source("users")
            .select(col("id"))
            .where(col("age") >= param("min_age", 18))
            .where(col("active") == param("active", True))
        )
        query2 = source("premium").select(col("user_id")).where(col("tier") == param("tier", "gold"))

        result = render(query1 & query2)

        assert result.query == sql("""
            (SELECT id FROM users WHERE age >= $min_age AND active = $active)
            INTERSECT
            (SELECT user_id FROM premium WHERE tier = $tier)
        """)
        assert result.params == {"min_age": 18, "active": True, "tier": "gold"}


def describe_with_source():
    def it_handles_bare_source_on_left():
        """
        (SELECT * FROM users) UNION (SELECT id FROM admins)
        """
        result = render(source("users") | source("admins").select(col("id")))

        assert result.query == sql("""
            (SELECT * FROM users) UNION (SELECT id FROM admins)
        """)

    def it_handles_bare_source_on_right():
        """
        (SELECT id FROM users) UNION (SELECT * FROM admins)
        """
        result = render(source("users").select(col("id")) | source("admins"))

        assert result.query == sql("""
            (SELECT id FROM users) UNION (SELECT * FROM admins)
        """)

    def it_handles_bare_source_on_both_sides():
        """
        (SELECT * FROM users) UNION (SELECT * FROM admins)
        """
        result = render(source("users") | source("admins"))

        assert result.query == sql("""
            (SELECT * FROM users) UNION (SELECT * FROM admins)
        """)
