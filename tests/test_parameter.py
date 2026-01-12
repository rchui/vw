"""Tests for vw/parameter.py module."""

import pytest

import vw
from vw.exceptions import UnsupportedParamStyleError


def describe_parameter() -> None:
    """Tests for Parameter class."""

    def it_renders_parameter_with_postgres_dialect() -> None:
        """Should render parameter with dollar prefix for PostgreSQL."""
        config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
        context = vw.RenderContext(config=config)
        param = vw.param("age", 25)
        assert param.__vw_render__(context) == "$age"
        assert context.params == {"age": 25}

    def it_renders_parameter_with_sqlserver_dialect() -> None:
        """Should render parameter with at-sign prefix for SQL Server."""
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        context = vw.RenderContext(config=config)
        param = vw.param("age", 25)
        assert param.__vw_render__(context) == "@age"
        assert context.params == {"age": 25}

    def it_renders_string_parameter(render_context: vw.RenderContext) -> None:
        """Should render string parameter."""
        param = vw.param("name", "Alice")
        assert param.__vw_render__(render_context) == "$name"
        assert render_context.params == {"name": "Alice"}

    def it_renders_float_parameter(render_context: vw.RenderContext) -> None:
        """Should render float parameter."""
        param = vw.param("price", 19.99)
        assert param.__vw_render__(render_context) == "$price"
        assert render_context.params == {"price": 19.99}

    def it_renders_bool_parameter(render_context: vw.RenderContext) -> None:
        """Should render boolean parameter."""
        param = vw.param("active", True)
        assert param.__vw_render__(render_context) == "$active"
        assert render_context.params == {"active": True}

    def it_allows_reusing_same_parameter(render_context: vw.RenderContext) -> None:
        """Should allow using the same parameter multiple times."""
        param = vw.param("threshold", 100)
        result1 = param.__vw_render__(render_context)
        result2 = param.__vw_render__(render_context)
        assert result1 == "$threshold"
        assert result2 == "$threshold"
        assert render_context.params == {"threshold": 100}


def describe_param_function() -> None:
    """Tests for param() function."""

    def it_creates_parameter() -> None:
        """Should create Parameter instance."""
        param = vw.param("user_id", 42)
        assert isinstance(param, vw.Parameter)
        assert param.name == "user_id"
        assert param.value == 42

    def it_rejects_unsupported_types() -> None:
        """Should raise TypeError for unsupported value types."""
        with pytest.raises(TypeError, match="Unsupported parameter type: list"):
            vw.param("data", [1, 2, 3])  # type: ignore

        with pytest.raises(TypeError, match="Unsupported parameter type: dict"):
            vw.param("config", {"key": "value"})  # type: ignore


def describe_parameter_rendering() -> None:
    """Tests for rendering Parameter objects."""

    @pytest.mark.parametrize(
        "style, expected_placeholder",
        [
            (vw.render.ParamStyle.COLON, ":age"),
            (vw.render.ParamStyle.DOLLAR, "$age"),
            (vw.render.ParamStyle.AT, "@age"),
            (vw.render.ParamStyle.PYFORMAT, "%(age)s"),
        ],
    )
    def it_renders_with_style_overrides(style, expected_placeholder) -> None:
        """Should render parameter with the correct placeholder for each style override."""
        param = vw.param("age", 25)
        config = vw.RenderConfig(param_style=style)
        context = vw.RenderContext(config=config)
        assert param.__vw_render__(context) == expected_placeholder
        assert context.params == {"age": 25}

    def it_raises_error_for_unsupported_param_style() -> None:
        """Should raise UnsupportedParamStyleError for an unknown style."""
        config = vw.RenderConfig(param_style="invalid_style")  # type: ignore
        context = vw.RenderContext(config=config)
        param = vw.param("age", 25)
        with pytest.raises(
            UnsupportedParamStyleError,
            match="Unsupported parameter style: invalid_style",
        ):
            param.__vw_render__(context)
