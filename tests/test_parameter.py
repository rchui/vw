"""Tests for vw/parameter.py module."""

import pytest

import vw


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
