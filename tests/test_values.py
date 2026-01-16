"""Unit tests for VALUES clause."""

import pytest

import vw
from vw.values import Values


def describe_values():
    """Tests for Values class."""

    def it_renders_single_row(render_context: vw.RenderContext) -> None:
        result = vw.values({"id": 1, "name": "Alice"}).alias("t").__vw_render__(render_context)
        assert result == "(VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
        assert render_context.params == {"_v0_0_id": 1, "_v0_1_name": "Alice"}

    def it_renders_multiple_rows(render_context: vw.RenderContext) -> None:
        result = vw.values(
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ).alias("t").__vw_render__(render_context)
        assert result == "(VALUES ($_v0_0_id, $_v0_1_name), ($_v1_0_id, $_v1_1_name)) AS t(id, name)"
        assert render_context.params == {
            "_v0_0_id": 1,
            "_v0_1_name": "Alice",
            "_v1_0_id": 2,
            "_v1_1_name": "Bob",
        }

    def it_renders_with_expression_values(render_context: vw.RenderContext) -> None:
        result = vw.values(
            {"id": 1, "created": vw.col("NOW()")},
        ).alias("t").__vw_render__(render_context)
        assert result == "(VALUES ($_v0_0_id, NOW())) AS t(id, created)"
        assert render_context.params == {"_v0_0_id": 1}

    def it_renders_with_none_values(render_context: vw.RenderContext) -> None:
        result = vw.values({"id": 1, "name": None}).alias("t").__vw_render__(render_context)
        assert result == "(VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
        assert render_context.params == {"_v0_0_id": 1, "_v0_1_name": None}

    def it_renders_various_types(render_context: vw.RenderContext) -> None:
        result = vw.values(
            {"int": 42, "float": 3.14, "str": "hello", "bool": True},
        ).alias("t").__vw_render__(render_context)
        assert result == "(VALUES ($_v0_0_int, $_v0_1_float, $_v0_2_str, $_v0_3_bool)) AS t(int, float, str, bool)"
        assert render_context.params == {
            "_v0_0_int": 42,
            "_v0_1_float": 3.14,
            "_v0_2_str": "hello",
            "_v0_3_bool": True,
        }

    def it_raises_on_empty_rows() -> None:
        v = Values(rows=(), _alias="t")
        context = vw.RenderContext(config=vw.RenderConfig())
        with pytest.raises(ValueError, match="VALUES requires at least one row"):
            v.__vw_render__(context)

    def it_raises_on_missing_alias() -> None:
        v = vw.values({"id": 1})
        context = vw.RenderContext(config=vw.RenderConfig())
        with pytest.raises(ValueError, match="VALUES requires an alias"):
            v.__vw_render__(context)


def describe_values_factory():
    """Tests for values() factory function."""

    def it_creates_values_with_single_row() -> None:
        v = vw.values({"id": 1})
        assert v.rows == ({"id": 1},)
        assert v._alias is None

    def it_creates_values_with_multiple_rows() -> None:
        v = vw.values({"id": 1}, {"id": 2}, {"id": 3})
        assert v.rows == ({"id": 1}, {"id": 2}, {"id": 3})
        assert v._alias is None


def describe_values_alias():
    """Tests for Values.alias() method."""

    def it_sets_alias() -> None:
        v = vw.values({"id": 1}).alias("users")
        assert v._alias == "users"
        assert v.rows == ({"id": 1},)

    def it_returns_new_instance() -> None:
        v1 = vw.values({"id": 1})
        v2 = v1.alias("users")
        assert v1 is not v2
        assert v1._alias is None
        assert v2._alias == "users"
