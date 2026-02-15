"""Tests for core raw SQL states (RawExpr and RawSource)."""

import pytest

from vw.core.render import ParamStyle, RenderConfig, RenderContext
from vw.core.states import (
    Alias,
    Column,
    Function,
    Literal,
    Operator,
    Parameter,
    RawExpr,
    RawSource,
)
from vw.postgres import F, col, lit, param, raw, ref, render
from vw.postgres.render import render_raw_expr, render_raw_source


@pytest.fixture
def ctx() -> RenderContext:
    """Create a render context for testing."""
    return RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))


# --- RawExpr State Tests --------------------------------------------------- #


def describe_raw_expr_state() -> None:
    """Test RawExpr state creation and basic properties."""

    def it_creates_raw_expr_state() -> None:
        """Test creating a RawExpr state directly."""
        state = RawExpr(sql="generate_series(1, 10)", params=())
        assert state.sql == "generate_series(1, 10)"
        assert state.params == ()

    def it_creates_raw_expr_with_single_param() -> None:
        """Test creating a RawExpr state with one parameter."""
        state = RawExpr(
            sql="UPPER({name})",
            params=(("name", Column(name="username")),),
        )
        assert state.sql == "UPPER({name})"
        assert len(state.params) == 1
        assert state.params[0][0] == "name"
        assert isinstance(state.params[0][1], Column)
        assert state.params[0][1].name == "username"

    def it_creates_raw_expr_with_multiple_params() -> None:
        """Test creating a RawExpr state with multiple parameters."""
        state = RawExpr(
            sql="{x} @> {y}",
            params=(
                ("x", Column(name="tags")),
                ("y", Parameter(name="tag", value="python")),
            ),
        )
        assert state.sql == "{x} @> {y}"
        assert len(state.params) == 2
        assert state.params[0][0] == "x"
        assert state.params[1][0] == "y"

    def it_creates_raw_expr_with_complex_expressions() -> None:
        """Test creating a RawExpr state with complex nested expressions."""
        state = RawExpr(
            sql="GREATEST({a}, {b}, {c})",
            params=(
                ("a", Column(name="x")),
                ("b", Column(name="y")),
                ("c", Literal(value=0)),
            ),
        )
        assert state.sql == "GREATEST({a}, {b}, {c})"
        assert len(state.params) == 3


def describe_raw_expr_rendering() -> None:
    """Test RawExpr rendering to SQL."""

    def it_renders_simple_expression(ctx: RenderContext) -> None:
        """Test rendering a simple RawExpr without parameters."""
        state = RawExpr(sql="generate_series(1, 10)", params=())
        assert render_raw_expr(state, ctx) == "generate_series(1, 10)"

    def it_renders_with_single_placeholder(ctx: RenderContext) -> None:
        """Test rendering RawExpr with one placeholder."""
        state = RawExpr(
            sql="UPPER({name})",
            params=(("name", Column(name="username")),),
        )
        assert render_raw_expr(state, ctx) == "UPPER(username)"

    def it_renders_with_multiple_placeholders(ctx: RenderContext) -> None:
        """Test rendering RawExpr with multiple placeholders."""
        state = RawExpr(
            sql="{a} @> {b}",
            params=(
                ("a", Column(name="tags")),
                ("b", Column(name="query")),
            ),
        )
        assert render_raw_expr(state, ctx) == "tags @> query"

    def it_renders_with_parameter(ctx: RenderContext) -> None:
        """Test rendering RawExpr with a query parameter."""
        state = RawExpr(
            sql="custom_func({val})",
            params=(("val", Parameter(name="input", value=42)),),
        )
        assert render_raw_expr(state, ctx) == "custom_func($input)"
        assert ctx.params == {"input": 42}

    def it_renders_with_literal(ctx: RenderContext) -> None:
        """Test rendering RawExpr with a literal value."""
        state = RawExpr(
            sql="COALESCE({col}, {default})",
            params=(
                ("col", Column(name="value")),
                ("default", Literal(value=0)),
            ),
        )
        assert render_raw_expr(state, ctx) == "COALESCE(value, 0)"
        assert ctx.params == {}

    def it_renders_with_mixed_types(ctx: RenderContext) -> None:
        """Test rendering RawExpr with columns, parameters, and literals."""
        state = RawExpr(
            sql="GREATEST({a}, {b}, {c})",
            params=(
                ("a", Column(name="score")),
                ("b", Parameter(name="min_score", value=50)),
                ("c", Literal(value=0)),
            ),
        )
        assert render_raw_expr(state, ctx) == "GREATEST(score, $min_score, 0)"
        assert ctx.params == {"min_score": 50}

    def it_renders_with_function_call(ctx: RenderContext) -> None:
        """Test rendering RawExpr with a function call as parameter."""
        state = RawExpr(
            sql="{func} > {threshold}",
            params=(
                ("func", Function(name="LENGTH", args=(Column(name="name"),))),
                ("threshold", Parameter(name="min_len", value=10)),
            ),
        )
        assert render_raw_expr(state, ctx) == "LENGTH(name) > $min_len"
        assert ctx.params == {"min_len": 10}

    def it_renders_with_binary_operation(ctx: RenderContext) -> None:
        """Test rendering RawExpr with binary operation as parameter."""
        state = RawExpr(
            sql="SQRT({expr})",
            params=(
                (
                    "expr",
                    Operator(
                        operator="+",
                        left=Column(name="x"),
                        right=Column(name="y"),
                    ),
                ),
            ),
        )
        assert render_raw_expr(state, ctx) == "SQRT(x + y)"

    def it_renders_repeated_placeholder() -> None:
        """Test rendering RawExpr with same placeholder used multiple times."""
        state = RawExpr(
            sql="{x} + {x} * 2",
            params=(("x", Column(name="value")),),
        )
        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        # Each placeholder should be substituted independently
        assert render_raw_expr(state, ctx) == "value + value * 2"

    def it_preserves_sql_syntax(ctx: RenderContext) -> None:
        """Test that RawExpr preserves special SQL syntax like operators."""
        state = RawExpr(
            sql="{arr} @> ARRAY[{val}]",
            params=(
                ("arr", Column(name="tags")),
                ("val", Parameter(name="search", value="python")),
            ),
        )
        assert render_raw_expr(state, ctx) == "tags @> ARRAY[$search]"


# --- RawSource State Tests ------------------------------------------------- #


def describe_raw_source_state() -> None:
    """Test RawSource state creation and basic properties."""

    def it_creates_raw_source_state() -> None:
        """Test creating a RawSource state directly."""
        state = RawSource(sql="generate_series(1, 10) AS t(n)", params=())
        assert state.sql == "generate_series(1, 10) AS t(n)"
        assert state.params == ()
        assert state.alias is None

    def it_creates_raw_source_with_alias() -> None:
        """Test creating a RawSource state with alias."""
        state = RawSource(
            sql="unnest(ARRAY[1,2,3])",
            params=(),
            alias="t",
        )
        assert state.sql == "unnest(ARRAY[1,2,3])"
        assert state.alias == "t"

    def it_creates_raw_source_with_params() -> None:
        """Test creating a RawSource state with parameters."""
        state = RawSource(
            sql="generate_series(1, {n}) AS t(num)",
            params=(("n", Parameter(name="max", value=10)),),
        )
        assert state.sql == "generate_series(1, {n}) AS t(num)"
        assert len(state.params) == 1
        assert state.params[0][0] == "n"

    def it_creates_raw_source_with_multiple_params() -> None:
        """Test creating a RawSource state with multiple parameters."""
        state = RawSource(
            sql="generate_series({start}, {end}) AS t(num)",
            params=(
                ("start", Parameter(name="min_val", value=1)),
                ("end", Parameter(name="max_val", value=100)),
            ),
        )
        assert state.sql == "generate_series({start}, {end}) AS t(num)"
        assert len(state.params) == 2


def describe_raw_source_rendering() -> None:
    """Test RawSource rendering to SQL."""

    def it_renders_simple_source(ctx: RenderContext) -> None:
        """Test rendering a simple RawSource without parameters."""
        state = RawSource(sql="generate_series(1, 10) AS t(n)", params=())
        assert render_raw_source(state, ctx) == "generate_series(1, 10) AS t(n)"

    def it_renders_with_alias(ctx: RenderContext) -> None:
        """Test rendering RawSource with alias."""
        state = RawSource(
            sql="unnest(ARRAY[1,2,3])",
            params=(),
            alias="t",
        )
        assert render_raw_source(state, ctx) == "unnest(ARRAY[1,2,3]) AS t"

    def it_renders_with_single_placeholder(ctx: RenderContext) -> None:
        """Test rendering RawSource with placeholder."""
        state = RawSource(
            sql="generate_series(1, {n}) AS t(num)",
            params=(("n", Parameter(name="max", value=10)),),
        )
        assert render_raw_source(state, ctx) == "generate_series(1, $max) AS t(num)"
        assert ctx.params == {"max": 10}

    def it_renders_with_multiple_placeholders(ctx: RenderContext) -> None:
        """Test rendering RawSource with multiple placeholders."""
        state = RawSource(
            sql="generate_series({start}, {end}) AS t(num)",
            params=(
                ("start", Parameter(name="min_val", value=1)),
                ("end", Parameter(name="max_val", value=100)),
            ),
        )
        assert render_raw_source(state, ctx) == "generate_series($min_val, $max_val) AS t(num)"
        assert ctx.params == {"min_val": 1, "max_val": 100}

    def it_renders_with_column_reference(ctx: RenderContext) -> None:
        """Test rendering RawSource with column reference in placeholder."""
        state = RawSource(
            sql="unnest({arr}) AS t(elem)",
            params=(("arr", Column(name="array_col")),),
        )
        assert render_raw_source(state, ctx) == "unnest(array_col) AS t(elem)"

    def it_renders_with_alias_and_params(ctx: RenderContext) -> None:
        """Test rendering RawSource with both alias and parameters."""
        state = RawSource(
            sql="generate_series(1, {n})",
            params=(("n", Parameter(name="count", value=5)),),
            alias="nums",
        )
        assert render_raw_source(state, ctx) == "generate_series(1, $count) AS nums"
        assert ctx.params == {"count": 5}

    def it_renders_lateral_unnest(ctx: RenderContext) -> None:
        """Test rendering LATERAL unnest pattern."""
        state = RawSource(
            sql="LATERAL unnest({arr}) AS t(elem)",
            params=(("arr", Column(name="tags")),),
        )
        assert render_raw_source(state, ctx) == "LATERAL unnest(tags) AS t(elem)"


# --- Factory API Tests ----------------------------------------------------- #


def describe_raw_expr_factory() -> None:
    """Test raw.expr() factory function."""

    def it_returns_expression() -> None:
        """raw.expr() should return an Expression."""
        expr = raw.expr("generate_series(1, 10)")
        # Check that it has the expected Expression interface
        assert hasattr(expr, "state")
        assert hasattr(expr, "alias")

    def it_creates_raw_expr_state() -> None:
        """raw.expr() should create RawExpr state."""
        expr = raw.expr("generate_series(1, 10)")
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "generate_series(1, 10)"
        assert expr.state.params == ()

    def it_accepts_kwargs_as_params() -> None:
        """raw.expr() should accept kwargs as named parameters."""
        expr = raw.expr("{a} @> {b}", a=col("tags"), b=param("tag", "python"))
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "{a} @> {b}"
        assert len(expr.state.params) == 2

    def it_stores_params_as_tuples() -> None:
        """raw.expr() should store params as (name, state) tuples."""
        expr = raw.expr("{x} + {y}", x=col("a"), y=col("b"))
        assert isinstance(expr.state, RawExpr)
        assert len(expr.state.params) == 2
        assert expr.state.params[0][0] == "x"
        assert isinstance(expr.state.params[0][1], Column)
        assert expr.state.params[1][0] == "y"
        assert isinstance(expr.state.params[1][1], Column)

    def it_renders_via_factory() -> None:
        """Expression from raw.expr() should render correctly."""
        expr = raw.expr("UPPER({name})", name=col("username"))
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "UPPER(username)"
        assert result.params == {}

    def it_renders_with_params_via_factory() -> None:
        """Expression from raw.expr() should render with parameters."""
        expr = raw.expr("custom_func({val})", val=param("input", 42))
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "custom_func($input)"
        assert result.params == {"input": 42}

    def it_works_with_multiple_params() -> None:
        """raw.expr() should handle multiple parameters correctly."""
        expr = raw.expr(
            "GREATEST({a}, {b}, {c})",
            a=col("x"),
            b=col("y"),
            c=param("default", 0),
        )
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "GREATEST(x, y, $default)"
        assert result.params == {"default": 0}

    def it_can_be_aliased() -> None:
        """Expression from raw.expr() should support .alias()."""
        expr = raw.expr("generate_series(1, 10)").alias("num")
        assert isinstance(expr.state, Alias)
        result = render(expr)
        assert result.query == "generate_series(1, 10) AS num"

    def it_can_be_used_in_comparison() -> None:
        """Expression from raw.expr() should work in comparisons."""
        expr = raw.expr("LENGTH({s})", s=col("name")) > lit(5)
        assert isinstance(expr.state, Operator)
        result = render(expr)
        assert result.query == "LENGTH(name) > 5"

    def it_works_in_select() -> None:
        """raw.expr() should work in SELECT clause."""
        query = ref("users").select(
            col("id"),
            raw.expr("UPPER({name})", name=col("username")).alias("upper_name"),
        )
        result = render(query)
        assert "SELECT id, UPPER(username) AS upper_name" in result.query
        assert "FROM users" in result.query

    def it_works_in_where() -> None:
        """raw.expr() should work in WHERE clause."""
        query = (
            ref("users")
            .select(col("id"))
            .where(raw.expr("{tags} @> {val}", tags=col("tags"), val=param("tag", "python")))
        )
        result = render(query)
        assert "WHERE tags @> $tag" in result.query
        assert result.params == {"tag": "python"}


def describe_raw_source_factory() -> None:
    """Test raw.rowset() factory function."""

    def it_returns_rowset() -> None:
        """raw.rowset() should return a RowSet."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        # Check that it has the expected RowSet interface
        assert hasattr(source, "state")
        assert hasattr(source, "select")

    def it_creates_raw_source_state() -> None:
        """raw.rowset() should create RawSource state."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        assert isinstance(source.state, RawSource)
        assert source.state.sql == "generate_series(1, 10) AS t(n)"

    def it_accepts_kwargs_as_params() -> None:
        """raw.rowset() should accept kwargs as named parameters."""
        source = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
        assert isinstance(source.state, RawSource)
        assert source.state.sql == "generate_series(1, {n}) AS t(num)"
        assert len(source.state.params) == 1

    def it_renders_via_factory() -> None:
        """RowSet from raw.rowset() should render correctly."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        result = render(source.select(col("n")))
        assert "FROM generate_series(1, 10) AS t(n)" in result.query

    def it_renders_with_params_via_factory() -> None:
        """RowSet from raw.rowset() should render with parameters."""
        source = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
        result = render(source.select(col("num")))
        assert "FROM generate_series(1, $max) AS t(num)" in result.query
        assert result.params == {"max": 10}

    def it_can_be_aliased() -> None:
        """RowSet from raw.rowset() should support .alias()."""
        source = raw.rowset("unnest(ARRAY[1,2,3])").alias("t")
        result = render(source.select(col("*")))
        assert "FROM unnest(ARRAY[1,2,3]) AS t" in result.query

    def it_can_be_selected_from() -> None:
        """raw.rowset() should work as a table source."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        query = source.select(col("n"))
        result = render(query)
        assert result.query == "SELECT n FROM generate_series(1, 10) AS t(n)"

    def it_works_with_where() -> None:
        """raw.rowset() should work with WHERE clause."""
        source = raw.rowset("generate_series(1, 10) AS t(n)")
        query = source.select(col("n")).where(col("n") > lit(5))
        result = render(query)
        assert "SELECT n FROM generate_series(1, 10) AS t(n)" in result.query
        assert "WHERE n > 5" in result.query

    def it_works_in_join() -> None:
        """raw.rowset() should work in JOIN clause."""
        users = ref("users")
        series = raw.rowset("generate_series(1, 10) AS t(n)")
        query = users.join.inner(series, on=[col("user_id") == col("n")]).select(col("*"))
        result = render(query)
        assert "FROM users" in result.query
        assert "INNER JOIN generate_series(1, 10) AS t(n)" in result.query


def describe_raw_func_factory() -> None:
    """Test raw.func() factory function."""

    def it_returns_expression() -> None:
        """raw.func() should return an Expression."""
        expr = raw.func("custom_function")
        # Check that it has the expected Expression interface
        assert hasattr(expr, "state")
        assert hasattr(expr, "alias")

    def it_creates_function_state() -> None:
        """raw.func() should create Function state."""
        expr = raw.func("gen_random_uuid")
        assert isinstance(expr.state, Function)
        assert expr.state.name == "GEN_RANDOM_UUID"

    def it_accepts_args() -> None:
        """raw.func() should accept positional arguments."""
        expr = raw.func("custom_hash", col("email"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "CUSTOM_HASH"
        assert len(expr.state.args) == 1

    def it_renders_no_args() -> None:
        """raw.func() without args should render correctly."""
        expr = raw.func("gen_random_uuid")
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "GEN_RANDOM_UUID()"

    def it_renders_with_args() -> None:
        """raw.func() with args should render correctly."""
        expr = raw.func("custom_hash", col("email"))
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "CUSTOM_HASH(email)"

    def it_renders_with_multiple_args() -> None:
        """raw.func() with multiple args should render correctly."""
        expr = raw.func("custom_func", col("a"), col("b"), param("c", 42))
        result = render(expr)  # type: ignore[arg-type]
        assert result.query == "CUSTOM_FUNC(a, b, $c)"
        assert result.params == {"c": 42}

    def it_uppercases_function_name() -> None:
        """raw.func() should uppercase function names."""
        expr = raw.func("my_custom_function")
        assert isinstance(expr.state, Function)
        assert expr.state.name == "MY_CUSTOM_FUNCTION"


# --- Integration Tests ----------------------------------------------------- #


def describe_raw_integration() -> None:
    """Test raw SQL integration with other vw features."""

    def it_combines_raw_expr_with_functions() -> None:
        """raw.expr() should work with F.* functions."""
        query = ref("posts").select(
            col("id"),
            F.count().alias("total"),
            raw.expr(
                "MAX({created}) FILTER (WHERE {active})", created=col("created_at"), active=col("is_active")
            ).alias("max_active"),
        )
        result = render(query)
        assert "SELECT id, COUNT(*) AS total, MAX(created_at) FILTER (WHERE is_active) AS max_active" in result.query

    def it_uses_raw_source_with_cte() -> None:
        """raw.rowset() should work with CTEs."""
        from vw.postgres import cte

        series = cte("nums", raw.rowset("generate_series(1, 10) AS t(n)").select(col("n")))
        query = series.select(col("n"))
        result = render(query)
        assert "WITH nums AS (SELECT n FROM generate_series(1, 10) AS t(n))" in result.query
        assert "SELECT n FROM nums" in result.query

    def it_nests_raw_expr_in_expressions() -> None:
        """raw.expr() should be nestable in other expressions."""
        expr = F.coalesce(raw.expr("custom_func({x})", x=col("value")), lit(0))
        result = render(expr)
        assert result.query == "COALESCE(custom_func(value), 0)"

    def it_combines_multiple_raw_sources() -> None:
        """Multiple raw.rowset() calls should work together."""
        s1 = raw.rowset("generate_series(1, 5) AS t1(n)")
        s2 = raw.rowset("generate_series(6, 10) AS t2(n)")
        query = s1.join.inner(s2, on=[col("t1.n") < col("t2.n")]).select(col("t1.n"), col("t2.n"))
        result = render(query)
        assert "FROM generate_series(1, 5) AS t1(n)" in result.query
        assert "INNER JOIN generate_series(6, 10) AS t2(n)" in result.query
