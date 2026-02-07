"""Integration tests for set operation SQL rendering."""

from tests.utils import sql
from vw.postgres import col, param, ref, render


def describe_union():
    def it_builds_basic_union():
        expected_sql = """
        (SELECT id FROM users) UNION (SELECT id FROM admins)
        """

        query1 = ref("users").select(col("id"))
        query2 = ref("admins").select(col("id"))

        result = render(query1 | query2)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_union_all():
        expected_sql = """
        (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
        """

        query1 = ref("users").select(col("id"))
        query2 = ref("admins").select(col("id"))

        result = render(query1 + query2)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_union_with_where():
        expected_sql = """
        (SELECT id FROM users WHERE active = $active) UNION (SELECT id FROM admins)
        """

        query1 = ref("users").select(col("id")).where(col("active") == param("active", True))
        query2 = ref("admins").select(col("id"))

        result = render(query1 | query2)

        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}


def describe_intersect():
    def it_builds_basic_intersect():
        expected_sql = """
        (SELECT id FROM users) INTERSECT (SELECT user_id FROM banned)
        """

        query1 = ref("users").select(col("id"))
        query2 = ref("banned").select(col("user_id"))

        result = render(query1 & query2)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_intersect_with_conditions():
        expected_sql = """
        (SELECT id FROM users WHERE age >= $min_age)
        INTERSECT
        (SELECT user_id FROM banned WHERE reason = $reason)
        """

        query1 = ref("users").select(col("id")).where(col("age") >= param("min_age", 18))
        query2 = ref("banned").select(col("user_id")).where(col("reason") == param("reason", "spam"))

        result = render(query1 & query2)

        assert result.query == sql(expected_sql)
        assert result.params == {"min_age": 18, "reason": "spam"}


def describe_except():
    def it_builds_basic_except():
        expected_sql = """
        (SELECT id FROM users) EXCEPT (SELECT user_id FROM banned)
        """

        query1 = ref("users").select(col("id"))
        query2 = ref("banned").select(col("user_id"))

        result = render(query1 - query2)

        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_builds_except_with_conditions():
        expected_sql = """
        (SELECT id FROM users WHERE active = $active)
        EXCEPT
        (SELECT user_id FROM banned)
        """

        query1 = ref("users").select(col("id")).where(col("active") == param("active", True))
        query2 = ref("banned").select(col("user_id"))

        result = render(query1 - query2)

        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}


def describe_chaining():
    def it_chains_multiple_unions():
        expected_sql = """
        ((SELECT id FROM users) UNION (SELECT id FROM admins)) UNION (SELECT id FROM guests)
        """

        users = ref("users").select(col("id"))
        admins = ref("admins").select(col("id"))
        guests = ref("guests").select(col("id"))

        result = render((users | admins) | guests)

        assert result.query == sql(expected_sql)

    def it_chains_mixed_operations():
        expected_sql = """
        ((SELECT id FROM users) UNION (SELECT id FROM admins))
        EXCEPT
        (SELECT user_id FROM banned)
        """

        users = ref("users").select(col("id"))
        admins = ref("admins").select(col("id"))
        banned = ref("banned").select(col("user_id"))

        result = render((users | admins) - banned)

        assert result.query == sql(expected_sql)

    def it_handles_complex_nesting():
        expected_sql = """
        (SELECT id FROM users)
        UNION
        ((SELECT id FROM admins) EXCEPT (SELECT user_id FROM banned))
        """

        users = ref("users").select(col("id"))
        admins = ref("admins").select(col("id"))
        banned = ref("banned").select(col("user_id"))

        result = render(users | (admins - banned))

        assert result.query == sql(expected_sql)


def describe_parameters():
    def it_preserves_parameters_across_union():
        expected_sql = """
        (SELECT id FROM users WHERE active = $active)
        UNION
        (SELECT id FROM admins WHERE role = $role)
        """

        query1 = ref("users").select(col("id")).where(col("active") == param("active", True))
        query2 = ref("admins").select(col("id")).where(col("role") == param("role", "admin"))

        result = render(query1 | query2)

        assert result.query == sql(expected_sql)
        assert result.params == {"active": True, "role": "admin"}

    def it_merges_parameters_from_both_sides():
        expected_sql = """
        (SELECT id FROM users WHERE age >= $min_age AND active = $active)
        INTERSECT
        (SELECT user_id FROM premium WHERE tier = $tier)
        """

        query1 = (
            ref("users")
            .select(col("id"))
            .where(col("age") >= param("min_age", 18))
            .where(col("active") == param("active", True))
        )
        query2 = ref("premium").select(col("user_id")).where(col("tier") == param("tier", "gold"))

        result = render(query1 & query2)

        assert result.query == sql(expected_sql)
        assert result.params == {"min_age": 18, "active": True, "tier": "gold"}


def describe_with_ref():
    def it_handles_bare_ref_on_left():
        expected_sql = """
        (SELECT * FROM users) UNION (SELECT id FROM admins)
        """

        result = render(ref("users") | ref("admins").select(col("id")))

        assert result.query == sql(expected_sql)

    def it_handles_bare_ref_on_right():
        expected_sql = """
        (SELECT id FROM users) UNION (SELECT * FROM admins)
        """

        result = render(ref("users").select(col("id")) | ref("admins"))

        assert result.query == sql(expected_sql)

    def it_handles_bare_ref_on_both_sides():
        expected_sql = """
        (SELECT * FROM users) UNION (SELECT * FROM admins)
        """

        result = render(ref("users") | ref("admins"))

        assert result.query == sql(expected_sql)
