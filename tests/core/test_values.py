"""Tests for values() factory and VALUES clause rendering."""

import pytest

from vw.core.states import Statement, Values
from vw.postgres import col, param, ref, render, values


def describe_values_factory() -> None:
    def it_creates_values_with_single_row() -> None:
        v = values("t", {"id": 1})
        assert isinstance(v.state, Values)
        assert v.state.rows == ({"id": 1},)
        assert v.state.alias == "t"

    def it_creates_values_with_multiple_rows() -> None:
        v = values("t", {"id": 1}, {"id": 2}, {"id": 3})
        assert isinstance(v.state, Values)
        assert v.state.rows == ({"id": 1}, {"id": 2}, {"id": 3})
        assert v.state.alias == "t"


def describe_values_rendering() -> None:
    def it_renders_single_row() -> None:
        result = render(values("t", {"id": 1, "name": "Alice"}).select(col("id"), col("name")))
        assert result.query == "SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
        assert result.params == {"_v0_0_id": 1, "_v0_1_name": "Alice"}

    def it_renders_multiple_rows() -> None:
        result = render(
            values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}).select(col("id"), col("name"))
        )
        assert result.query == (
            "SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name), ($_v1_0_id, $_v1_1_name)) AS t(id, name)"
        )
        assert result.params == {
            "_v0_0_id": 1,
            "_v0_1_name": "Alice",
            "_v1_0_id": 2,
            "_v1_1_name": "Bob",
        }

    def it_renders_none_values() -> None:
        result = render(values("t", {"id": 1, "name": None}).select(col("*")))
        assert result.query == "SELECT * FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
        assert result.params == {"_v0_0_id": 1, "_v0_1_name": None}

    def it_renders_various_types() -> None:
        result = render(values("t", {"i": 42, "f": 3.14, "s": "hello", "b": True}).select(col("*")))
        assert result.query == "SELECT * FROM (VALUES ($_v0_0_i, $_v0_1_f, $_v0_2_s, $_v0_3_b)) AS t(i, f, s, b)"
        assert result.params == {"_v0_0_i": 42, "_v0_1_f": 3.14, "_v0_2_s": "hello", "_v0_3_b": True}

    def it_renders_with_expression_value() -> None:
        result = render(values("t", {"id": 1, "ts": param("now", "NOW()")}).select(col("*")))
        assert result.query == "SELECT * FROM (VALUES ($_v0_0_id, $now)) AS t(id, ts)"
        assert result.params == {"_v0_0_id": 1, "now": "NOW()"}

    def it_renders_standalone_with_from_prefix() -> None:
        result = render(values("t", {"id": 1}))
        assert result.query == "FROM (VALUES ($_v0_0_id)) AS t(id)"
        assert result.params == {"_v0_0_id": 1}


def describe_values_errors() -> None:
    def it_raises_on_empty_rows() -> None:
        with pytest.raises(ValueError, match="VALUES requires at least one row"):
            render(values("t").select(col("*")))

    def it_raises_on_missing_alias() -> None:
        # Construct state directly to bypass factory validation
        from vw.core.render import ParamStyle, RenderConfig, RenderContext
        from vw.postgres.render import render_values

        v_state = Values(rows=({"id": 1},), alias=None)
        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        with pytest.raises(ValueError, match="VALUES requires an alias"):
            render_values(v_state, ctx)


def describe_values_as_source() -> None:
    def it_builds_statement_from_values() -> None:
        v = values("t", {"id": 1})
        q = v.select(col("id"))
        assert isinstance(q.state, Statement)
        assert isinstance(q.state.source, Values)

    def it_renders_in_join() -> None:
        result = render(
            ref("users")
            .join.inner(
                values("v", {"user_id": 1}, {"user_id": 2}),
                on=[col("users.id") == col("v.user_id")],
            )
            .select(col("users.name"))
        )
        assert result.query == (
            "SELECT users.name FROM users "
            "INNER JOIN (VALUES ($_v0_0_user_id), ($_v1_0_user_id)) AS v(user_id) "
            "ON (users.id = v.user_id)"
        )
        assert result.params == {"_v0_0_user_id": 1, "_v1_0_user_id": 2}

    def it_renders_in_cte() -> None:
        from vw.postgres import cte

        ids = cte("ids", values("v", {"id": 1}, {"id": 2}).select(col("id")))
        result = render(ids.select(col("id")))
        assert result.query == (
            "WITH ids AS (SELECT id FROM (VALUES ($_v0_0_id), ($_v1_0_id)) AS v(id)) SELECT id FROM ids"
        )
        assert result.params == {"_v0_0_id": 1, "_v1_0_id": 2}
