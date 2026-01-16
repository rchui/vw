"""Integration tests for VALUES clause."""

import vw
from tests.utils import sql


def describe_values_as_source():
    """Tests for VALUES as a row source."""

    def it_selects_from_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM (VALUES ($_v0_0_id, $_v0_1_name), ($_v1_0_id, $_v1_1_name)) AS users(id, name)
        """
        result = (
            vw.values({"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
            .alias("users")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v0_1_name": "Alice", "_v1_0_id": 2, "_v1_1_name": "Bob"},
        )

    def it_selects_specific_columns_from_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS users(id, name)
        """
        result = (
            vw.values({"id": 1, "name": "Alice"})
            .alias("users")
            .select(vw.col("id"), vw.col("name"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v0_1_name": "Alice"},
        )

    def it_filters_values_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM (VALUES ($_v0_0_id, $_v0_1_active), ($_v1_0_id, $_v1_1_active)) AS users(id, active)
            WHERE (active = true)
        """
        result = (
            vw.values({"id": 1, "active": True}, {"id": 2, "active": False})
            .alias("users")
            .select(vw.col("*"))
            .where(vw.col("active") == vw.col("true"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v0_1_active": True, "_v1_0_id": 2, "_v1_1_active": False},
        )


def describe_values_in_joins():
    """Tests for VALUES in JOIN operations."""

    def it_joins_table_to_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, ids.id
            FROM users
            INNER JOIN (VALUES ($_v0_0_id), ($_v1_0_id)) AS ids(id) ON (users.id = ids.id)
        """
        users = vw.Source(name="users")
        ids = vw.values({"id": 1}, {"id": 2}).alias("ids")
        result = (
            users.join.inner(ids, on=[vw.col("users.id") == vw.col("ids.id")])
            .select(vw.col("users.name"), vw.col("ids.id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v1_0_id": 2},
        )

    def it_left_joins_to_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, overrides.value
            FROM users
            LEFT JOIN (VALUES ($_v0_0_user_id, $_v0_1_value)) AS overrides(user_id, value)
                ON (users.id = overrides.user_id)
        """
        users = vw.Source(name="users")
        overrides = vw.values({"user_id": 1, "value": 100}).alias("overrides")
        result = (
            users.join.left(overrides, on=[vw.col("users.id") == vw.col("overrides.user_id")])
            .select(vw.col("users.name"), vw.col("overrides.value"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_user_id": 1, "_v0_1_value": 100},
        )


def describe_values_with_expressions():
    """Tests for VALUES containing SQL expressions."""

    def it_renders_expressions_in_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM (VALUES ($_v0_0_id, NOW())) AS records(id, created_at)
        """
        result = (
            vw.values({"id": 1, "created_at": vw.col("NOW()")})
            .alias("records")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1},
        )

    def it_mixes_params_and_expressions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM (VALUES ($_v0_0_id, $_v0_1_name, CURRENT_DATE)) AS users(id, name, created)
        """
        result = (
            vw.values({"id": 1, "name": "Alice", "created": vw.col("CURRENT_DATE")})
            .alias("users")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v0_1_name": "Alice"},
        )


def describe_values_in_cte():
    """Tests for VALUES in CTEs."""

    def it_uses_values_in_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH ids AS (SELECT * FROM (VALUES ($_v0_0_id), ($_v1_0_id), ($_v2_0_id)) AS t(id))
            SELECT users.*
            FROM users
            INNER JOIN ids ON (users.id = ids.id)
        """
        ids_values = vw.values({"id": 1}, {"id": 2}, {"id": 3}).alias("t")
        ids_cte = vw.cte("ids", ids_values.select(vw.col("*")))
        users = vw.Source(name="users")
        result = (
            users.join.inner(ids_cte, on=[vw.col("users.id") == vw.col("ids.id")])
            .select(vw.col("users.*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v1_0_id": 2, "_v2_0_id": 3},
        )


def describe_values_with_various_types():
    """Tests for VALUES with different Python types."""

    def it_handles_none_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS users(id, name)
        """
        result = (
            vw.values({"id": 1, "name": None})
            .alias("users")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_id": 1, "_v0_1_name": None},
        )

    def it_handles_numeric_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM (VALUES ($_v0_0_int, $_v0_1_float, $_v0_2_neg)) AS nums(int, float, neg)
        """
        result = (
            vw.values({"int": 42, "float": 3.14159, "neg": -100})
            .alias("nums")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_int": 42, "_v0_1_float": 3.14159, "_v0_2_neg": -100},
        )

    def it_handles_boolean_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM (VALUES ($_v0_0_a, $_v0_1_b)) AS flags(a, b)
        """
        result = (
            vw.values({"a": True, "b": False})
            .alias("flags")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_a": True, "_v0_1_b": False},
        )

    def it_handles_string_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM (VALUES ($_v0_0_msg)) AS texts(msg)
        """
        result = (
            vw.values({"msg": "Hello, World!"})
            .alias("texts")
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"_v0_0_msg": "Hello, World!"},
        )
