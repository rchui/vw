"""PostgreSQL SQL rendering."""

from __future__ import annotations

from vw.core.render import SQL, ParamStyle, RenderConfig, RenderContext
from vw.core.states import (
    Add,
    Alias,
    And,
    Asc,
    Between,
    Cast,
    Column,
    Desc,
    Divide,
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    IsIn,
    IsNotIn,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    Not,
    NotBetween,
    NotEquals,
    NotLike,
    Or,
    Parameter,
    Source,
    Statement,
    Subtract,
)
from vw.postgres.base import Expression, RowSet


def render(obj: RowSet | Expression, *, config: RenderConfig | None = None) -> SQL:
    """Render a RowSet or Expression to PostgreSQL SQL.

    Args:
        obj: A RowSet or Expression to render.
        config: Optional rendering configuration (defaults to PostgreSQL DOLLAR style).

    Returns:
        SQL result with query string and parameters dict.

    Example:
        >>> from vw.postgres import source, col, param, render
        >>> result = render(source("users").select(col("id")).where(col("age") >= param("min_age", 18)))
        >>> result.query
        'SELECT id FROM users WHERE age >= $min_age'
        >>> result.params
        {'min_age': 18}
    """
    # Create rendering context with PostgreSQL defaults
    ctx = RenderContext(config=config or RenderConfig(param_style=ParamStyle.DOLLAR))

    # Top-level Source should render with FROM
    if isinstance(obj.state, Source):
        query = f"FROM {render_source(obj.state, ctx)}"
    else:
        query = render_state(obj.state, ctx)

    return SQL(query=query, params=ctx.params)


def render_state(state: object, ctx: RenderContext) -> str:
    """Render a state object to SQL.

    Args:
        state: A state object (Source, Statement, Column, etc.).
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.

    Raises:
        TypeError: If the state type is unknown.
    """
    # --- Core Query States --------------------------------------------- #
    if isinstance(state, Statement):
        return render_statement(state, ctx)
    elif isinstance(state, Source):
        return render_source(state, ctx)
    elif isinstance(state, Column):
        return render_column(state)
    elif isinstance(state, Parameter):
        return render_parameter(state, ctx)

    # --- Comparison Operators ------------------------------------------ #
    elif isinstance(state, Equals):
        return f"{render_state(state.left, ctx)} = {render_state(state.right, ctx)}"
    elif isinstance(state, NotEquals):
        return f"{render_state(state.left, ctx)} <> {render_state(state.right, ctx)}"
    elif isinstance(state, LessThan):
        return f"{render_state(state.left, ctx)} < {render_state(state.right, ctx)}"
    elif isinstance(state, LessThanOrEqual):
        return f"{render_state(state.left, ctx)} <= {render_state(state.right, ctx)}"
    elif isinstance(state, GreaterThan):
        return f"{render_state(state.left, ctx)} > {render_state(state.right, ctx)}"
    elif isinstance(state, GreaterThanOrEqual):
        return f"{render_state(state.left, ctx)} >= {render_state(state.right, ctx)}"

    # --- Arithmetic Operators ------------------------------------------ #
    elif isinstance(state, Add):
        return f"{render_state(state.left, ctx)} + {render_state(state.right, ctx)}"
    elif isinstance(state, Subtract):
        return f"{render_state(state.left, ctx)} - {render_state(state.right, ctx)}"
    elif isinstance(state, Multiply):
        return f"{render_state(state.left, ctx)} * {render_state(state.right, ctx)}"
    elif isinstance(state, Divide):
        return f"{render_state(state.left, ctx)} / {render_state(state.right, ctx)}"
    elif isinstance(state, Modulo):
        return f"{render_state(state.left, ctx)} % {render_state(state.right, ctx)}"

    # --- Logical Operators --------------------------------------------- #
    elif isinstance(state, And):
        return f"({render_state(state.left, ctx)}) AND ({render_state(state.right, ctx)})"
    elif isinstance(state, Or):
        return f"({render_state(state.left, ctx)}) OR ({render_state(state.right, ctx)})"
    elif isinstance(state, Not):
        return f"NOT ({render_state(state.operand, ctx)})"

    # --- Pattern Matching ---------------------------------------------- #
    elif isinstance(state, Like):
        return f"{render_state(state.left, ctx)} LIKE {render_state(state.right, ctx)}"
    elif isinstance(state, NotLike):
        return f"{render_state(state.left, ctx)} NOT LIKE {render_state(state.right, ctx)}"
    elif isinstance(state, IsIn):
        values = ", ".join(render_state(v, ctx) for v in state.values)
        return f"{render_state(state.expr, ctx)} IN ({values})"
    elif isinstance(state, IsNotIn):
        values = ", ".join(render_state(v, ctx) for v in state.values)
        return f"{render_state(state.expr, ctx)} NOT IN ({values})"
    elif isinstance(state, Between):
        return f"{render_state(state.expr, ctx)} BETWEEN {render_state(state.lower_bound, ctx)} AND {render_state(state.upper_bound, ctx)}"
    elif isinstance(state, NotBetween):
        return f"{render_state(state.expr, ctx)} NOT BETWEEN {render_state(state.lower_bound, ctx)} AND {render_state(state.upper_bound, ctx)}"

    # --- NULL Checks --------------------------------------------------- #
    elif isinstance(state, IsNull):
        return f"{render_state(state.expr, ctx)} IS NULL"
    elif isinstance(state, IsNotNull):
        return f"{render_state(state.expr, ctx)} IS NOT NULL"

    # --- Expression Modifiers ------------------------------------------ #
    elif isinstance(state, Alias):
        return f"{render_state(state.expr, ctx)} AS {state.name}"
    elif isinstance(state, Cast):
        # PostgreSQL uses :: syntax for casting
        return f"{render_state(state.expr, ctx)}::{state.data_type}"
    elif isinstance(state, Asc):
        return f"{render_state(state.expr, ctx)} ASC"
    elif isinstance(state, Desc):
        return f"{render_state(state.expr, ctx)} DESC"

    else:
        raise TypeError(f"Unknown state type: {type(state)}")


def render_statement(stmt: Statement, ctx: RenderContext) -> str:
    """Render a Statement to SQL.

    Args:
        stmt: A Statement to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    parts = []

    # SELECT clause (with optional DISTINCT)
    if stmt.columns:
        select_clause = "SELECT"
        if stmt.distinct:
            select_clause = "SELECT DISTINCT"
        cols = ", ".join(render_state(col.state, ctx) for col in stmt.columns)
        parts.append(f"{select_clause} {cols}")

    # FROM clause (source can be Source or Statement for subqueries)
    if isinstance(stmt.source, Source):
        source_sql = render_source(stmt.source, ctx)
    else:  # Statement (subquery)
        source_sql = f"({render_statement(stmt.source, ctx)})"
        if stmt.source.alias:
            source_sql += f" AS {stmt.source.alias}"
    parts.append(f"FROM {source_sql}")

    # WHERE clause
    if stmt.where_conditions:
        conditions = " AND ".join(render_state(cond.state, ctx) for cond in stmt.where_conditions)
        parts.append(f"WHERE {conditions}")

    # GROUP BY clause
    if stmt.group_by_columns:
        cols = ", ".join(render_state(col.state, ctx) for col in stmt.group_by_columns)
        parts.append(f"GROUP BY {cols}")

    # HAVING clause
    if stmt.having_conditions:
        conditions = " AND ".join(render_state(cond.state, ctx) for cond in stmt.having_conditions)
        parts.append(f"HAVING {conditions}")

    # ORDER BY clause
    if stmt.order_by_columns:
        cols = ", ".join(render_state(col.state, ctx) for col in stmt.order_by_columns)
        parts.append(f"ORDER BY {cols}")

    # LIMIT/OFFSET clause
    if stmt.limit:
        parts.append(f"LIMIT {stmt.limit.count}")
        if stmt.limit.offset:
            parts.append(f"OFFSET {stmt.limit.offset}")

    return " ".join(parts)


def render_source(source: Source, ctx: RenderContext) -> str:
    """Render a Source to SQL.

    Args:
        source: A Source to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "users" or "users AS u").
    """
    if source.alias:
        return f"{source.name} AS {source.alias}"
    return source.name


def render_column(col: Column) -> str:
    """Render a Column to SQL.

    Args:
        col: A Column to render.

    Returns:
        The SQL string (e.g., "id" or "id AS user_id").
    """
    if col.alias:
        return f"{col.name} AS {col.alias}"
    return col.name


def render_parameter(param: Parameter, ctx: RenderContext) -> str:
    """Render a Parameter to SQL using parameter binding.

    Args:
        param: A Parameter to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL placeholder string (e.g., "$age" for PostgreSQL).
    """
    # Add parameter to context and get placeholder
    return ctx.add_param(param.name, param.value)
