"""Integration tests for PostgreSQL-specific convenience functions."""

from tests.utils import sql
from vw.postgres import F, col, lit, param, ref, render


def describe_tier1_functions() -> None:
    """Test Tier 1 (must-have) PostgreSQL functions."""

    def describe_gen_random_uuid() -> None:
        """Test GEN_RANDOM_UUID function."""

        def it_renders_basic_usage() -> None:
            """Test basic gen_random_uuid usage."""
            expected_sql = """SELECT GEN_RANDOM_UUID() FROM users"""

            q = ref("users").select(F.gen_random_uuid())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_alias() -> None:
            """Test gen_random_uuid with alias."""
            expected_sql = """SELECT GEN_RANDOM_UUID() AS id FROM users"""

            q = ref("users").select(F.gen_random_uuid().alias("id"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_other_columns() -> None:
            """Test gen_random_uuid with other columns."""
            expected_sql = """SELECT GEN_RANDOM_UUID() AS id, name FROM users"""

            q = ref("users").select(F.gen_random_uuid().alias("id"), col("name"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_array_agg() -> None:
        """Test ARRAY_AGG function."""

        def it_renders_basic_usage() -> None:
            """Test basic array_agg."""
            expected_sql = """SELECT ARRAY_AGG(name) FROM users"""

            q = ref("users").select(F.array_agg(col("name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_order_by() -> None:
            """Test array_agg with ORDER BY."""
            expected_sql = """SELECT ARRAY_AGG(name ORDER BY name) FROM users"""

            q = ref("users").select(F.array_agg(col("name"), order_by=[col("name")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_order_by_desc() -> None:
            """Test array_agg with ORDER BY DESC."""
            expected_sql = """SELECT ARRAY_AGG(name ORDER BY name DESC) FROM users"""

            q = ref("users").select(F.array_agg(col("name"), order_by=[col("name").desc()]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_multiple_order_by_columns() -> None:
            """Test array_agg with multiple ORDER BY columns."""
            expected_sql = """SELECT ARRAY_AGG(name ORDER BY department, name) FROM users"""

            q = ref("users").select(F.array_agg(col("name"), order_by=[col("department"), col("name")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_distinct() -> None:
            """Test array_agg with DISTINCT."""
            expected_sql = """SELECT ARRAY_AGG(DISTINCT tag) FROM posts"""

            q = ref("posts").select(F.array_agg(col("tag"), distinct=True))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_distinct_and_order_by() -> None:
            """Test array_agg with DISTINCT and ORDER BY."""
            expected_sql = """SELECT ARRAY_AGG(DISTINCT tag ORDER BY tag) FROM posts"""

            q = ref("posts").select(F.array_agg(col("tag"), distinct=True, order_by=[col("tag")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_group_by() -> None:
            """Test array_agg with GROUP BY."""
            expected_sql = """SELECT user_id, ARRAY_AGG(tag ORDER BY tag) FROM posts GROUP BY user_id"""

            q = (
                ref("posts")
                .select(col("user_id"), F.array_agg(col("tag"), order_by=[col("tag")]))
                .group_by(col("user_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_string_agg() -> None:
        """Test STRING_AGG function."""

        def it_renders_basic_usage() -> None:
            """Test basic string_agg."""
            expected_sql = """SELECT STRING_AGG(name, $_lit_0) FROM users"""

            q = ref("users").select(F.string_agg(col("name"), lit(", ")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": ", "}

        def it_renders_with_order_by() -> None:
            """Test string_agg with ORDER BY."""
            expected_sql = """SELECT STRING_AGG(name, $_lit_0 ORDER BY name) FROM users"""

            q = ref("users").select(F.string_agg(col("name"), lit(", "), order_by=[col("name")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": ", "}

        def it_renders_with_group_by() -> None:
            """Test string_agg with GROUP BY."""
            expected_sql = (
                """SELECT department, STRING_AGG(name, $_lit_0 ORDER BY name) FROM users GROUP BY department"""
            )

            q = (
                ref("users")
                .select(col("department"), F.string_agg(col("name"), lit(", "), order_by=[col("name")]))
                .group_by(col("department"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": ", "}


def describe_tier2_functions() -> None:
    """Test Tier 2 (high-value) PostgreSQL functions."""

    def describe_json_build_object() -> None:
        """Test JSON_BUILD_OBJECT function."""

        def it_renders_basic_usage() -> None:
            """Test basic json_build_object."""
            expected_sql = """SELECT JSON_BUILD_OBJECT($_lit_0, id, $_lit_1, name) FROM users"""

            q = ref("users").select(F.json_build_object(lit("id"), col("id"), lit("name"), col("name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": "id", "_lit_1": "name"}

        def it_renders_many_fields() -> None:
            """Test json_build_object with many fields."""
            expected_sql = (
                """SELECT JSON_BUILD_OBJECT($_lit_0, id, $_lit_1, name, $_lit_2, email, $_lit_3, status) FROM users"""
            )

            q = ref("users").select(
                F.json_build_object(
                    lit("id"),
                    col("id"),
                    lit("name"),
                    col("name"),
                    lit("email"),
                    col("email"),
                    lit("status"),
                    col("status"),
                )
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": "id", "_lit_1": "name", "_lit_2": "email", "_lit_3": "status"}

    def describe_json_agg() -> None:
        """Test JSON_AGG function."""

        def it_renders_basic_usage() -> None:
            """Test basic json_agg."""
            expected_sql = """SELECT JSON_AGG(data) FROM logs"""

            q = ref("logs").select(F.json_agg(col("data")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_order_by() -> None:
            """Test json_agg with ORDER BY."""
            expected_sql = """SELECT JSON_AGG(data ORDER BY created_at) FROM logs"""

            q = ref("logs").select(F.json_agg(col("data"), order_by=[col("created_at")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_json_build_object() -> None:
            """Test json_agg wrapping json_build_object."""
            expected_sql = """SELECT JSON_AGG(JSON_BUILD_OBJECT($_lit_0, id, $_lit_1, name) ORDER BY name) FROM users"""

            json_obj = F.json_build_object(lit("id"), col("id"), lit("name"), col("name"))
            q = ref("users").select(F.json_agg(json_obj, order_by=[col("name")]))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"_lit_0": "id", "_lit_1": "name"}

        def it_renders_with_group_by() -> None:
            """Test json_agg with GROUP BY."""
            expected_sql = """SELECT user_id, JSON_AGG(data ORDER BY created_at) FROM logs GROUP BY user_id"""

            q = (
                ref("logs")
                .select(col("user_id"), F.json_agg(col("data"), order_by=[col("created_at")]))
                .group_by(col("user_id"))
            )
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_unnest() -> None:
        """Test UNNEST function."""

        def it_renders_in_select() -> None:
            """Test unnest in SELECT clause."""
            expected_sql = """SELECT UNNEST(tags) FROM posts"""

            q = ref("posts").select(F.unnest(col("tags")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_renders_with_alias() -> None:
            """Test unnest with alias."""
            expected_sql = """SELECT UNNEST(tags) AS tag FROM posts"""

            q = ref("posts").select(F.unnest(col("tags")).alias("tag"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}


def describe_order_by_with_filter() -> None:
    """Test ORDER BY combined with FILTER clause."""

    def it_renders_array_agg_with_filter_and_order() -> None:
        """Test array_agg with both FILTER and ORDER BY."""
        expected_sql = """SELECT ARRAY_AGG(name ORDER BY name) FILTER (WHERE active = $status) FROM users"""

        agg = F.array_agg(col("name"), order_by=[col("name")]).filter(col("active") == param("status", True))
        q = ref("users").select(agg)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"status": True}

    def it_renders_string_agg_with_filter_and_order() -> None:
        """Test string_agg with both FILTER and ORDER BY."""
        expected_sql = """SELECT STRING_AGG(name, $_lit_0 ORDER BY name) FILTER (WHERE active = $status) FROM users"""

        agg = F.string_agg(col("name"), lit(", "), order_by=[col("name")]).filter(
            col("active") == param("status", True)
        )
        q = ref("users").select(agg)
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"_lit_0": ", ", "status": True}
