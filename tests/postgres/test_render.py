"""Tests for vw/postgres/render.py module."""

import pytest

from vw.core.render import RenderConfig, RenderContext
from vw.core.states import (
    CTE,
    Alias,
    Asc,
    Between,
    Cast,
    Column,
    CurrentRow,
    Desc,
    Exists,
    Following,
    FrameClause,
    Function,
    IsIn,
    IsNotIn,
    IsNotNull,
    IsNull,
    Like,
    Not,
    NotBetween,
    NotLike,
    Operator,
    Parameter,
    Preceding,
    Reference,
    SetOperation,
    Statement,
    UnboundedFollowing,
    UnboundedPreceding,
    WindowFunction,
)
from vw.postgres import F, col, cte, param, ref, render
from vw.postgres.render import (
    ParamStyle,
    render_column,
    render_exists,
    render_frame_clause,
    render_function,
    render_parameter,
    render_set_operation,
    render_source,
    render_state,
    render_statement,
    render_with_clause,
)


@pytest.fixture
def ctx() -> RenderContext:
    return RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))


def describe_render() -> None:
    def it_renders_ref() -> None:
        result = render(ref("users"))
        assert result.query == "FROM users"
        assert result.params == {}

    def it_renders_statement() -> None:
        result = render(ref("users").select(col("id")))
        assert result.query == "SELECT id FROM users"
        assert result.params == {}

    def it_renders_expression() -> None:
        result = render(col("id"))
        assert result.query == "id"
        assert result.params == {}

    def it_renders_set_operation() -> None:
        q = ref("a").select(col("id")) | ref("b").select(col("id"))
        result = render(q)
        assert result.query == "(SELECT id FROM a) UNION (SELECT id FROM b)"

    def it_prepends_with_clause_for_ctes() -> None:
        active = cte("active", ref("users").select(col("id")))
        result = render(active.select(col("id")))
        assert result.query.startswith("WITH active AS")

    def it_raises_on_unknown_state_type(ctx: RenderContext) -> None:
        with pytest.raises(TypeError, match="Unknown state type"):
            render_state(42, ctx)


def describe_render_state() -> None:
    """Tests for render_state() dispatch."""

    def describe_comparison_operators() -> None:
        def it_renders_equals(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="=", left=Column(name="a"), right=Column(name="b")), ctx) == "a = b"

        def it_renders_not_equals(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="<>", left=Column(name="a"), right=Column(name="b")), ctx) == "a <> b"

        def it_renders_less_than(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="<", left=Column(name="a"), right=Column(name="b")), ctx) == "a < b"

        def it_renders_less_than_or_equal(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="<=", left=Column(name="a"), right=Column(name="b")), ctx) == "a <= b"

        def it_renders_greater_than(ctx: RenderContext) -> None:
            assert render_state(Operator(operator=">", left=Column(name="a"), right=Column(name="b")), ctx) == "a > b"

        def it_renders_greater_than_or_equal(ctx: RenderContext) -> None:
            assert render_state(Operator(operator=">=", left=Column(name="a"), right=Column(name="b")), ctx) == "a >= b"

    def describe_arithmetic_operators() -> None:
        def it_renders_add(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="+", left=Column(name="a"), right=Column(name="b")), ctx) == "a + b"

        def it_renders_subtract(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="-", left=Column(name="a"), right=Column(name="b")), ctx) == "a - b"

        def it_renders_multiply(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="*", left=Column(name="a"), right=Column(name="b")), ctx) == "a * b"

        def it_renders_divide(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="/", left=Column(name="a"), right=Column(name="b")), ctx) == "a / b"

        def it_renders_modulo(ctx: RenderContext) -> None:
            assert render_state(Operator(operator="%", left=Column(name="a"), right=Column(name="b")), ctx) == "a % b"

    def describe_logical_operators() -> None:
        def it_renders_and(ctx: RenderContext) -> None:
            assert (
                render_state(Operator(operator="AND", left=Column(name="a"), right=Column(name="b")), ctx)
                == "(a) AND (b)"
            )

        def it_renders_or(ctx: RenderContext) -> None:
            assert (
                render_state(Operator(operator="OR", left=Column(name="a"), right=Column(name="b")), ctx)
                == "(a) OR (b)"
            )

        def it_renders_not(ctx: RenderContext) -> None:
            assert render_state(Not(operand=Column(name="a")), ctx) == "NOT (a)"

    def describe_pattern_matching() -> None:
        def it_renders_like(ctx: RenderContext) -> None:
            assert render_state(Like(left=Column(name="name"), right=Column(name="'A%'")), ctx) == "name LIKE 'A%'"

        def it_renders_not_like(ctx: RenderContext) -> None:
            assert (
                render_state(NotLike(left=Column(name="name"), right=Column(name="'A%'")), ctx) == "name NOT LIKE 'A%'"
            )

        def it_renders_is_in_with_values(ctx: RenderContext) -> None:
            state = IsIn(expr=Column(name="id"), values=(Column(name="1"), Column(name="2")))
            assert render_state(state, ctx) == "id IN (1, 2)"

        def it_renders_is_in_with_subquery(ctx: RenderContext) -> None:
            result = render(ref("users").select(col("id")).where(col("id").is_in(ref("t").select(col("id")))))
            assert result.query == "SELECT id FROM users WHERE id IN (SELECT id FROM t)"

        def it_renders_is_not_in(ctx: RenderContext) -> None:
            state = IsNotIn(expr=Column(name="id"), values=(Column(name="1"),))
            assert render_state(state, ctx) == "id NOT IN (1)"

        def it_renders_between(ctx: RenderContext) -> None:
            state = Between(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))
            assert render_state(state, ctx) == "age BETWEEN 18 AND 65"

        def it_renders_not_between(ctx: RenderContext) -> None:
            state = NotBetween(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))
            assert render_state(state, ctx) == "age NOT BETWEEN 18 AND 65"

    def describe_null_checks() -> None:
        def it_renders_is_null(ctx: RenderContext) -> None:
            assert render_state(IsNull(expr=Column(name="x")), ctx) == "x IS NULL"

        def it_renders_is_not_null(ctx: RenderContext) -> None:
            assert render_state(IsNotNull(expr=Column(name="x")), ctx) == "x IS NOT NULL"

    def describe_expression_modifiers() -> None:
        def it_renders_alias(ctx: RenderContext) -> None:
            assert render_state(Alias(expr=Column(name="id"), name="user_id"), ctx) == "id AS user_id"

        def it_renders_cast(ctx: RenderContext) -> None:
            assert render_state(Cast(expr=Column(name="x"), data_type="int"), ctx) == "x::int"

        def it_renders_asc(ctx: RenderContext) -> None:
            assert render_state(Asc(expr=Column(name="id")), ctx) == "id ASC"

        def it_renders_desc(ctx: RenderContext) -> None:
            assert render_state(Desc(expr=Column(name="id")), ctx) == "id DESC"

    def describe_frame_boundaries() -> None:
        def it_renders_unbounded_preceding(ctx: RenderContext) -> None:
            assert render_state(UnboundedPreceding(), ctx) == "UNBOUNDED PRECEDING"

        def it_renders_unbounded_following(ctx: RenderContext) -> None:
            assert render_state(UnboundedFollowing(), ctx) == "UNBOUNDED FOLLOWING"

        def it_renders_current_row(ctx: RenderContext) -> None:
            assert render_state(CurrentRow(), ctx) == "CURRENT ROW"

        def it_renders_preceding(ctx: RenderContext) -> None:
            assert render_state(Preceding(count=3), ctx) == "3 PRECEDING"

        def it_renders_following(ctx: RenderContext) -> None:
            assert render_state(Following(count=2), ctx) == "2 FOLLOWING"


def describe_render_source() -> None:
    def it_renders_reference(ctx: RenderContext) -> None:
        assert render_source(Reference(name="users"), ctx) == "users"

    def it_renders_aliased_reference(ctx: RenderContext) -> None:
        assert render_source(Reference(name="users", alias="u"), ctx) == "users AS u"

    def it_renders_statement_as_subquery(ctx: RenderContext) -> None:
        stmt = Statement(source=Reference(name="t"), columns=())
        assert render_source(stmt, ctx) == "(FROM t)"

    def it_renders_aliased_statement_as_subquery(ctx: RenderContext) -> None:
        stmt = Statement(source=Reference(name="t"), columns=(), alias="sub")
        assert render_source(stmt, ctx) == "(FROM t) AS sub"

    def it_renders_set_operation_as_subquery(ctx: RenderContext) -> None:
        setop = SetOperation(
            left=Statement(source=Reference(name="a"), columns=()),
            right=Statement(source=Reference(name="b"), columns=()),
            operator="UNION",
        )
        result = render_source(setop, ctx)
        assert result == "((FROM a) UNION (FROM b))"

    def it_registers_cte_and_returns_name(ctx: RenderContext) -> None:
        active = cte("active", ref("users").select(col("id")))
        assert isinstance(active.state, CTE)
        sql = render_source(active.state, ctx)
        assert sql == "active"
        assert len(ctx.ctes) == 1

    def it_renders_cte_with_alias(ctx: RenderContext) -> None:
        active = cte("active", ref("users").select(col("id"))).alias("a")
        assert isinstance(active.state, CTE)
        sql = render_source(active.state, ctx)
        assert sql == "active AS a"


def describe_render_column() -> None:
    def it_renders_simple_column() -> None:
        assert render_column(Column(name="id")) == "id"

    def it_renders_aliased_column() -> None:
        assert render_column(Column(name="id", alias="user_id")) == "id AS user_id"


def describe_render_parameter() -> None:
    def it_renders_dollar_placeholder(ctx: RenderContext) -> None:
        assert render_parameter(Parameter(name="age", value=25), ctx) == "$age"
        assert ctx.params == {"age": 25}


def describe_render_statement() -> None:
    def it_renders_from_only(ctx: RenderContext) -> None:
        stmt = Statement(source=Reference(name="users"), columns=())
        assert render_statement(stmt, ctx) == "FROM users"

    def it_renders_select(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id"), col("name")))
        assert result.query == "SELECT id, name FROM users"

    def it_renders_select_distinct(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id")).distinct())
        assert result.query == "SELECT DISTINCT id FROM users"

    def it_renders_where(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id")).where(col("active") == param("v", True)))
        assert result.query == "SELECT id FROM users WHERE active = $v"
        assert result.params == {"v": True}

    def it_renders_group_by(ctx: RenderContext) -> None:
        result = render(ref("orders").select(col("user_id")).group_by(col("user_id")))
        assert result.query == "SELECT user_id FROM orders GROUP BY user_id"

    def it_renders_having(ctx: RenderContext) -> None:
        result = render(
            ref("orders").select(col("user_id")).group_by(col("user_id")).having(F.count() > param("min", 5))
        )
        assert result.query == "SELECT user_id FROM orders GROUP BY user_id HAVING COUNT(*) > $min"

    def it_renders_order_by(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id")).order_by(col("name").asc()))
        assert result.query == "SELECT id FROM users ORDER BY name ASC"

    def it_renders_limit(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id")).limit(10))
        assert result.query == "SELECT id FROM users LIMIT 10"

    def it_renders_limit_with_offset(ctx: RenderContext) -> None:
        result = render(ref("users").select(col("id")).limit(10, offset=20))
        assert result.query == "SELECT id FROM users LIMIT 10 OFFSET 20"


def describe_render_function() -> None:
    def it_renders_count_star(ctx: RenderContext) -> None:
        assert render_function(Function(name="COUNT(*)"), ctx) == "COUNT(*)"

    def it_renders_count_distinct_star(ctx: RenderContext) -> None:
        assert render_function(Function(name="COUNT(DISTINCT *)"), ctx) == "COUNT(DISTINCT *)"

    def it_renders_count_distinct_expr(ctx: RenderContext) -> None:
        func = Function(name="COUNT(DISTINCT", args=(Column(name="id"),))
        assert render_function(func, ctx) == "COUNT(DISTINCT id)"

    def it_renders_function_with_args(ctx: RenderContext) -> None:
        func = Function(name="SUM", args=(Column(name="amount"),))
        assert render_function(func, ctx) == "SUM(amount)"

    def it_renders_function_with_int_args(ctx: RenderContext) -> None:
        func = Function(name="NTILE", args=(4,))
        assert render_function(func, ctx) == "NTILE(4)"

    def it_renders_no_arg_function(ctx: RenderContext) -> None:
        assert render_function(Function(name="NOW"), ctx) == "NOW()"

    def it_renders_filter_clause(ctx: RenderContext) -> None:
        func = Function(name="COUNT(*)", filter=Column(name="active"))
        assert render_function(func, ctx) == "COUNT(*) FILTER (WHERE active)"


def describe_render_window_function() -> None:
    def it_renders_with_partition_and_order(ctx: RenderContext) -> None:
        wf = WindowFunction(
            function=Function(name="ROW_NUMBER"),
            partition_by=(Column(name="dept"),),
            order_by=(Column(name="salary"),),
        )
        assert render_state(wf, ctx) == "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary)"

    def it_renders_with_frame(ctx: RenderContext) -> None:
        wf = WindowFunction(
            function=Function(name="SUM", args=(Column(name="x"),)),
            order_by=(Column(name="id"),),
            frame=FrameClause(mode="ROWS", start=UnboundedPreceding(), end=CurrentRow()),
        )
        assert render_state(wf, ctx) == "SUM(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

    def it_renders_empty_over(ctx: RenderContext) -> None:
        wf = WindowFunction(function=Function(name="ROW_NUMBER"))
        assert render_state(wf, ctx) == "ROW_NUMBER() OVER ()"


def describe_render_frame_clause() -> None:
    def it_renders_rows_frame(ctx: RenderContext) -> None:
        frame = FrameClause(mode="ROWS", start=UnboundedPreceding(), end=CurrentRow())
        assert render_frame_clause(frame, ctx) == "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"

    def it_renders_range_frame(ctx: RenderContext) -> None:
        frame = FrameClause(mode="RANGE", start=Preceding(count=1), end=Following(count=1))
        assert render_frame_clause(frame, ctx) == "RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING"

    def it_renders_exclude_clause(ctx: RenderContext) -> None:
        frame = FrameClause(mode="ROWS", start=UnboundedPreceding(), end=CurrentRow(), exclude="CURRENT ROW")
        assert render_frame_clause(frame, ctx) == "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW"


def describe_render_join() -> None:
    def it_renders_inner_join_with_on(ctx: RenderContext) -> None:
        result = render(
            ref("users")
            .alias("u")
            .join.inner(ref("orders").alias("o"), on=[col("u.id") == col("o.user_id")])
            .select(col("u.id"))
        )
        assert result.query == "SELECT u.id FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)"

    def it_renders_left_join(ctx: RenderContext) -> None:
        result = render(
            ref("users")
            .alias("u")
            .join.left(ref("orders").alias("o"), on=[col("u.id") == col("o.user_id")])
            .select(col("u.id"))
        )
        assert result.query == "SELECT u.id FROM users AS u LEFT JOIN orders AS o ON (u.id = o.user_id)"

    def it_renders_join_with_using(ctx: RenderContext) -> None:
        result = render(ref("users").join.inner(ref("orders"), using=[col("id")]).select(col("id")))
        assert result.query == "SELECT id FROM users INNER JOIN orders USING (id)"


def describe_render_exists() -> None:
    def it_renders_exists_with_statement(ctx: RenderContext) -> None:
        subq = ref("orders").select(col("id")).where(col("user_id") == col("1"))
        exists = Exists(subquery=subq.state)
        assert render_exists(exists, ctx) == "EXISTS (SELECT id FROM orders WHERE user_id = 1)"

    def it_renders_exists_with_reference(ctx: RenderContext) -> None:
        exists = Exists(subquery=Reference(name="orders"))
        assert render_exists(exists, ctx) == "EXISTS (SELECT * FROM orders)"


def describe_render_set_operation() -> None:
    def it_renders_union(ctx: RenderContext) -> None:
        left = Statement(source=Reference(name="a"), columns=())
        right = Statement(source=Reference(name="b"), columns=())
        setop = SetOperation(left=left, right=right, operator="UNION")
        assert render_set_operation(setop, ctx) == "(FROM a) UNION (FROM b)"

    def it_renders_union_all(ctx: RenderContext) -> None:
        q = ref("a").select(col("id")) + ref("b").select(col("id"))
        result = render(q)
        assert result.query == "(SELECT id FROM a) UNION ALL (SELECT id FROM b)"

    def it_renders_intersect(ctx: RenderContext) -> None:
        q = ref("a").select(col("id")) & ref("b").select(col("id"))
        result = render(q)
        assert result.query == "(SELECT id FROM a) INTERSECT (SELECT id FROM b)"

    def it_renders_except(ctx: RenderContext) -> None:
        q = ref("a").select(col("id")) - ref("b").select(col("id"))
        result = render(q)
        assert result.query == "(SELECT id FROM a) EXCEPT (SELECT id FROM b)"

    def it_wraps_reference_sides_in_select_star(ctx: RenderContext) -> None:
        setop = SetOperation(left=Reference(name="a"), right=Reference(name="b"), operator="UNION")
        assert render_set_operation(setop, ctx) == "(SELECT * FROM a) UNION (SELECT * FROM b)"


def describe_render_with_clause() -> None:
    def it_renders_with(ctx: RenderContext) -> None:
        ctx.register_cte("my_cte", "SELECT id FROM users", recursive=False)
        assert render_with_clause(ctx) == "WITH my_cte AS (SELECT id FROM users)"

    def it_renders_with_recursive(ctx: RenderContext) -> None:
        ctx.register_cte("tree", "SELECT id FROM nodes", recursive=True)
        assert render_with_clause(ctx) == "WITH RECURSIVE tree AS (SELECT id FROM nodes)"

    def it_renders_multiple_ctes(ctx: RenderContext) -> None:
        ctx.register_cte("a", "SELECT 1", recursive=False)
        ctx.register_cte("b", "SELECT 2", recursive=False)
        assert render_with_clause(ctx) == "WITH a AS (SELECT 1), b AS (SELECT 2)"
