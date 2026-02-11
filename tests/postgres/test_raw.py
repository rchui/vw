"""Unit tests for raw SQL API."""

import pytest

from vw.core.render import ParamStyle, RenderConfig, RenderContext
from vw.core.states import Column, Parameter, RawExpr, RawSource
from vw.postgres import col, param, raw
from vw.postgres.render import render_raw_expr, render_raw_source


@pytest.fixture
def ctx() -> RenderContext:
    """Create a render context for testing."""
    return RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))


def describe_raw_expr_state() -> None:
    """Test RawExpr state creation and rendering."""

    def it_creates_raw_expr_state() -> None:
        """Test creating a RawExpr state directly."""
        state = RawExpr(sql="generate_series(1, 10)", params=())
        assert state.sql == "generate_series(1, 10)"
        assert state.params == ()

    def it_creates_raw_expr_with_params() -> None:
        """Test creating a RawExpr state with parameters."""
        state = RawExpr(
            sql="{x} @> {y}",
            params=(("x", Column(name="tags")), ("y", Parameter(name="tag", value="python"))),
        )
        assert state.sql == "{x} @> {y}"
        assert len(state.params) == 2
        assert state.params[0][0] == "x"
        assert state.params[1][0] == "y"

    def it_renders_simple_expression(ctx: RenderContext) -> None:
        """Test rendering a simple RawExpr without parameters."""
        state = RawExpr(sql="generate_series(1, 10)", params=())
        assert render_raw_expr(state, ctx) == "generate_series(1, 10)"

    def it_renders_with_single_placeholder(ctx: RenderContext) -> None:
        """Test rendering RawExpr with one placeholder."""
        state = RawExpr(sql="UPPER({name})", params=(("name", Column(name="username")),))
        assert render_raw_expr(state, ctx) == "UPPER(username)"

    def it_renders_with_multiple_placeholders(ctx: RenderContext) -> None:
        """Test rendering RawExpr with multiple placeholders."""
        state = RawExpr(
            sql="{a} @> {b}",
            params=(("a", Column(name="tags")), ("b", Column(name="query"))),
        )
        assert render_raw_expr(state, ctx) == "tags @> query"

    def it_renders_with_parameter(ctx: RenderContext) -> None:
        """Test rendering RawExpr with a query parameter."""
        state = RawExpr(sql="custom_func({val})", params=(("val", Parameter(name="input", value=42)),))
        assert render_raw_expr(state, ctx) == "custom_func($input)"
        assert ctx.params == {"input": 42}


def describe_raw_source_state() -> None:
    """Test RawSource state creation and rendering."""

    def it_creates_raw_source_state() -> None:
        """Test creating a RawSource state directly."""
        state = RawSource(sql="generate_series(1, 10) AS t(n)", params=())
        assert state.sql == "generate_series(1, 10) AS t(n)"
        assert state.params == ()
        assert state.alias is None

    def it_creates_raw_source_with_alias() -> None:
        """Test creating a RawSource state with alias."""
        state = RawSource(sql="unnest(ARRAY[1,2,3])", params=(), alias="t")
        assert state.sql == "unnest(ARRAY[1,2,3])"
        assert state.alias == "t"

    def it_renders_simple_source(ctx: RenderContext) -> None:
        """Test rendering a simple RawSource without parameters."""
        state = RawSource(sql="generate_series(1, 10) AS t(n)", params=())
        assert render_raw_source(state, ctx) == "generate_series(1, 10) AS t(n)"

    def it_renders_with_alias(ctx: RenderContext) -> None:
        """Test rendering RawSource with alias."""
        state = RawSource(sql="unnest(ARRAY[1,2,3])", params=(), alias="t")
        assert render_raw_source(state, ctx) == "unnest(ARRAY[1,2,3]) AS t"

    def it_renders_with_placeholder(ctx: RenderContext) -> None:
        """Test rendering RawSource with placeholder."""
        state = RawSource(sql="generate_series(1, {n}) AS t(num)", params=(("n", Parameter(name="max", value=10)),))
        assert render_raw_source(state, ctx) == "generate_series(1, $max) AS t(num)"
        assert ctx.params == {"max": 10}


def describe_raw_factories() -> None:
    """Test raw API factory functions."""

    def it_creates_raw_expr_via_factory() -> None:
        """Test raw.expr() factory creates RawExpr."""
        expr = raw.expr("generate_series(1, 10)")
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "generate_series(1, 10)"
        assert expr.state.params == ()

    def it_creates_raw_expr_with_dependencies() -> None:
        """Test raw.expr() factory with kwargs."""
        expr = raw.expr("{a} @> {b}", a=col("tags"), b=param("tag", "python"))
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "{a} @> {b}"
        assert len(expr.state.params) == 2

    def it_creates_raw_source_via_factory() -> None:
        """Test raw.rowset() factory creates RawSource."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        assert isinstance(source.state, RawSource)
        assert source.state.sql == "generate_series(1, 10) AS t(n)"

    def it_creates_raw_source_with_dependencies() -> None:
        """Test raw.rowset() factory with kwargs."""
        source = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
        assert isinstance(source.state, RawSource)
        assert source.state.sql == "generate_series(1, {n}) AS t(num)"
        assert len(source.state.params) == 1
