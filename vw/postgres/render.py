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
    CurrentRow,
    Desc,
    Divide,
    Equals,
    Following,
    FrameClause,
    Function,
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
    Preceding,
    Source,
    Statement,
    Subtract,
    UnboundedFollowing,
    UnboundedPreceding,
    WindowFunction,
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
    match state:
        # --- Core Query States ----------------------------------------- #
        case Statement():
            return render_statement(state, ctx)
        case Source():
            return render_source(state, ctx)
        case Column():
            return render_column(state)
        case Parameter():
            return render_parameter(state, ctx)

        # --- Comparison Operators -------------------------------------- #
        case Equals():
            return f"{render_state(state.left, ctx)} = {render_state(state.right, ctx)}"
        case NotEquals():
            return f"{render_state(state.left, ctx)} <> {render_state(state.right, ctx)}"
        case LessThan():
            return f"{render_state(state.left, ctx)} < {render_state(state.right, ctx)}"
        case LessThanOrEqual():
            return f"{render_state(state.left, ctx)} <= {render_state(state.right, ctx)}"
        case GreaterThan():
            return f"{render_state(state.left, ctx)} > {render_state(state.right, ctx)}"
        case GreaterThanOrEqual():
            return f"{render_state(state.left, ctx)} >= {render_state(state.right, ctx)}"

        # --- Arithmetic Operators -------------------------------------- #
        case Add():
            return f"{render_state(state.left, ctx)} + {render_state(state.right, ctx)}"
        case Subtract():
            return f"{render_state(state.left, ctx)} - {render_state(state.right, ctx)}"
        case Multiply():
            return f"{render_state(state.left, ctx)} * {render_state(state.right, ctx)}"
        case Divide():
            return f"{render_state(state.left, ctx)} / {render_state(state.right, ctx)}"
        case Modulo():
            return f"{render_state(state.left, ctx)} % {render_state(state.right, ctx)}"

        # --- Logical Operators ----------------------------------------- #
        case And():
            return f"({render_state(state.left, ctx)}) AND ({render_state(state.right, ctx)})"
        case Or():
            return f"({render_state(state.left, ctx)}) OR ({render_state(state.right, ctx)})"
        case Not():
            return f"NOT ({render_state(state.operand, ctx)})"

        # --- Pattern Matching ------------------------------------------ #
        case Like():
            return f"{render_state(state.left, ctx)} LIKE {render_state(state.right, ctx)}"
        case NotLike():
            return f"{render_state(state.left, ctx)} NOT LIKE {render_state(state.right, ctx)}"
        case IsIn():
            values = ", ".join(render_state(v, ctx) for v in state.values)
            return f"{render_state(state.expr, ctx)} IN ({values})"
        case IsNotIn():
            values = ", ".join(render_state(v, ctx) for v in state.values)
            return f"{render_state(state.expr, ctx)} NOT IN ({values})"
        case Between():
            return f"{render_state(state.expr, ctx)} BETWEEN {render_state(state.lower_bound, ctx)} AND {render_state(state.upper_bound, ctx)}"
        case NotBetween():
            return f"{render_state(state.expr, ctx)} NOT BETWEEN {render_state(state.lower_bound, ctx)} AND {render_state(state.upper_bound, ctx)}"

        # --- NULL Checks ----------------------------------------------- #
        case IsNull():
            return f"{render_state(state.expr, ctx)} IS NULL"
        case IsNotNull():
            return f"{render_state(state.expr, ctx)} IS NOT NULL"

        # --- Expression Modifiers -------------------------------------- #
        case Alias():
            return f"{render_state(state.expr, ctx)} AS {state.name}"
        case Cast():
            # PostgreSQL uses :: syntax for casting
            return f"{render_state(state.expr, ctx)}::{state.data_type}"
        case Asc():
            return f"{render_state(state.expr, ctx)} ASC"
        case Desc():
            return f"{render_state(state.expr, ctx)} DESC"

        # --- Functions ------------------------------------------------- #
        case Function():
            return render_function(state, ctx)
        case WindowFunction():
            return render_window_function(state, ctx)

        # --- Frame Clauses --------------------------------------------- #
        case FrameClause():
            return render_frame_clause(state, ctx)
        case UnboundedPreceding():
            return "UNBOUNDED PRECEDING"
        case UnboundedFollowing():
            return "UNBOUNDED FOLLOWING"
        case CurrentRow():
            return "CURRENT ROW"
        case Preceding():
            return f"{state.count} PRECEDING"
        case Following():
            return f"{state.count} FOLLOWING"

        case _:
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


def render_function(func: Function, ctx: RenderContext) -> str:
    """Render a Function to SQL.

    Args:
        func: A Function to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    # Render function arguments
    if func.args:
        # Handle special cases for COUNT(DISTINCT ...)
        if func.name == "COUNT(DISTINCT":
            # COUNT(DISTINCT expr) - close the DISTINCT paren
            args_sql = ", ".join(render_state(arg, ctx) for arg in func.args)
            sql = f"COUNT(DISTINCT {args_sql})"
        else:
            # Normal function with args
            # Handle integer literals (for NTILE, LAG offset, etc.)
            rendered_args = []
            for arg in func.args:
                if isinstance(arg, int):
                    rendered_args.append(str(arg))
                else:
                    rendered_args.append(render_state(arg, ctx))
            args_sql = ", ".join(rendered_args)
            sql = f"{func.name}({args_sql})"
    else:
        # Function with no args
        # Handle special case for COUNT(*)
        if func.name == "COUNT(*)":
            sql = "COUNT(*)"
        elif func.name == "COUNT(DISTINCT *)":
            sql = "COUNT(DISTINCT *)"
        else:
            sql = f"{func.name}()"

    # Add FILTER clause if present
    if func.filter:
        filter_sql = render_state(func.filter, ctx)
        sql += f" FILTER (WHERE {filter_sql})"

    return sql


def render_window_function(wf: WindowFunction, ctx: RenderContext) -> str:
    """Render a WindowFunction to SQL.

    Args:
        wf: A WindowFunction to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    # Render the base function
    func_sql = render_state(wf.function, ctx)

    # Build OVER clause parts
    over_parts = []

    if wf.partition_by:
        cols = ", ".join(render_state(col, ctx) for col in wf.partition_by)
        over_parts.append(f"PARTITION BY {cols}")

    if wf.order_by:
        cols = ", ".join(render_state(col, ctx) for col in wf.order_by)
        over_parts.append(f"ORDER BY {cols}")

    if wf.frame:
        over_parts.append(render_state(wf.frame, ctx))

    over_clause = " ".join(over_parts)
    return f"{func_sql} OVER ({over_clause})"


def render_frame_clause(frame: FrameClause, ctx: RenderContext) -> str:
    """Render a FrameClause to SQL.

    Args:
        frame: A FrameClause to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    # Render frame boundaries
    start_sql = render_state(frame.start, ctx)
    end_sql = render_state(frame.end, ctx)

    sql = f"{frame.mode} BETWEEN {start_sql} AND {end_sql}"

    # Add EXCLUDE clause if present
    if frame.exclude:
        sql += f" EXCLUDE {frame.exclude}"

    return sql
