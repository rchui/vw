"""PostgreSQL SQL rendering."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vw.core.render import SQL, ParamStyle, RenderConfig, RenderContext

if TYPE_CHECKING:
    from vw.postgres.base import Expression

from vw.core.states import (
    CTE,
    Alias,
    Asc,
    Between,
    Case,
    Cast,
    Column,
    CurrentDate,
    CurrentRow,
    CurrentTime,
    CurrentTimestamp,
    Desc,
    Exists,
    Expr,
    Extract,
    Following,
    FrameClause,
    Function,
    IsIn,
    IsNotIn,
    IsNotNull,
    IsNull,
    Join,
    Like,
    Not,
    NotBetween,
    NotLike,
    Operator,
    Parameter,
    Preceding,
    Reference,
    ScalarSubquery,
    SetOperation,
    Statement,
    UnboundedFollowing,
    UnboundedPreceding,
    Values,
    WindowFunction,
)
from vw.postgres.base import RowSet
from vw.postgres.states import DateTrunc, Interval, Now


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
    ctx = RenderContext(config=config or RenderConfig(param_style=ParamStyle.DOLLAR))

    # Top-level Reference and Values render with FROM prefix; everything else renders directly
    if isinstance(obj.state, (Reference, Values)):
        query = f"FROM {render_state(obj.state, ctx)}"
    else:
        query = render_state(obj.state, ctx)

    if ctx.ctes:
        query = f"{render_with_clause(ctx)} {query}"

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
        case CTE():
            return render_cte(state, ctx)
        case Statement():
            return render_statement(state, ctx)
        case Reference():
            return render_source(state, ctx)
        case Values():
            return render_values(state, ctx)
        case Column():
            return render_column(state)
        case Parameter():
            return render_parameter(state, ctx)

        # --- Binary Operators ------------------------------------------ #
        case Operator():
            left = render_state(state.left, ctx)
            right = render_state(state.right, ctx)
            if state.operator in ("AND", "OR"):
                return f"({left}) {state.operator} ({right})"
            return f"{left} {state.operator} {right}"

        # --- Logical Operators ----------------------------------------- #
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

        # --- Date/Time Expressions ------------------------------------- #
        case Extract():
            return f"EXTRACT({state.field.upper()} FROM {render_state(state.expr, ctx)})"
        case CurrentTimestamp():
            return "CURRENT_TIMESTAMP"
        case CurrentDate():
            return "CURRENT_DATE"
        case CurrentTime():
            return "CURRENT_TIME"
        case Now():
            return "NOW()"
        case Interval():
            return f"INTERVAL '{state.amount} {state.unit}'"
        case DateTrunc():
            return f"DATE_TRUNC('{state.unit}', {render_state(state.expr, ctx)})"

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

        # --- Joins ----------------------------------------------------- #
        case Join():
            return render_join(state, ctx)

        # --- Conditional Expressions ----------------------------------- #
        case Case():
            whens = " ".join(
                f"WHEN {render_state(w.condition, ctx)} THEN {render_state(w.result, ctx)}" for w in state.whens
            )
            else_sql = f" ELSE {render_state(state.else_result, ctx)}" if state.else_result is not None else ""
            return f"CASE {whens}{else_sql} END"

        # --- Subquery Operators ---------------------------------------- #
        case ScalarSubquery():
            return f"({render_state(state.query, ctx)})"
        case Exists():
            return render_exists(state, ctx)

        # --- Set Operations -------------------------------------------- #
        case SetOperation():
            return render_set_operation(state, ctx)

        case _:
            raise TypeError(f"Unknown state type: {type(state)}")


def render_source(source: CTE | Statement | SetOperation | Reference | Values, ctx: RenderContext) -> str:
    """Render a source (CTE, Statement subquery, SetOperation, Reference, or Values) to SQL.

    For CTEs: registers the CTE body and returns the CTE name reference.
    For Statements/SetOperations: wraps in parens as a subquery.
    For References: renders the table/view name.
    For Values: renders the VALUES clause with alias and column list.
    """
    if isinstance(source, CTE):
        ctx.register_cte(source.name, render_state(source, ctx), source.recursive)
        if source.alias:
            return f"{source.name} AS {source.alias}"
        return source.name
    elif isinstance(source, (Statement, SetOperation)):
        sql = f"({render_state(source, ctx)})"
        if source.alias:
            return f"{sql} AS {source.alias}"
        return sql
    elif isinstance(source, Values):
        return render_values(source, ctx)
    else:
        if source.alias:
            return f"{source.name} AS {source.alias}"
        return source.name


def render_values(values_src: Values, ctx: RenderContext) -> str:
    """Render a VALUES clause to SQL.

    Args:
        values_src: A Values source to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "(VALUES ($1, $2)) AS t(id, name)").

    Raises:
        ValueError: If alias is missing or rows is empty.
    """
    if not values_src.alias:
        raise ValueError("VALUES requires an alias")
    if not values_src.rows:
        raise ValueError("VALUES requires at least one row")

    columns = list(values_src.rows[0].keys())
    row_sqls = []
    for row_idx, row in enumerate(values_src.rows):
        placeholders = []
        for col_idx, col_name in enumerate(columns):
            value = row[col_name]
            if isinstance(value, Expr):
                placeholders.append(render_state(value, ctx))
            else:
                param_name = f"_v{row_idx}_{col_idx}_{col_name}"
                placeholders.append(ctx.add_param(param_name, value))
        row_sqls.append(f"({', '.join(placeholders)})")

    values_sql = f"VALUES {', '.join(row_sqls)}"
    col_list = ", ".join(columns)
    return f"({values_sql}) AS {values_src.alias}({col_list})"


def render_cte(cte: CTE, ctx: RenderContext) -> str:
    """Render a CTE body.

    Args:
        cte: A CTE to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string for the CTE body.
    """
    # CTE source may be a set operation (e.g. UNION) or a standard statement
    if isinstance(cte.source, SetOperation):
        return render_state(cte.source, ctx)
    return render_statement(cte, ctx)


def render_statement(stmt: Statement, ctx: RenderContext) -> str:
    """Render a Statement to SQL.

    Args:
        stmt: A Statement to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    parts = []

    # SELECT clause (with optional DISTINCT or DISTINCT ON)
    if stmt.columns:
        if stmt.distinct and stmt.distinct.on:
            on_cols = ", ".join(render_state(e, ctx) for e in stmt.distinct.on)
            select_clause = f"SELECT DISTINCT ON ({on_cols})"
        elif stmt.distinct:
            select_clause = "SELECT DISTINCT"
        else:
            select_clause = "SELECT"
        cols = ", ".join(render_state(col, ctx) for col in stmt.columns)
        parts.append(f"{select_clause} {cols}")

    # FROM clause
    parts.append(f"FROM {render_source(stmt.source, ctx)}")

    # JOIN clauses
    for join in stmt.joins:
        parts.append(render_state(join, ctx))

    # WHERE clause
    if stmt.where_conditions:
        conditions = " AND ".join(render_state(cond, ctx) for cond in stmt.where_conditions)
        parts.append(f"WHERE {conditions}")

    # GROUP BY clause
    if stmt.group_by_columns:
        cols = ", ".join(render_state(col, ctx) for col in stmt.group_by_columns)
        parts.append(f"GROUP BY {cols}")

    # HAVING clause
    if stmt.having_conditions:
        conditions = " AND ".join(render_state(cond, ctx) for cond in stmt.having_conditions)
        parts.append(f"HAVING {conditions}")

    # ORDER BY clause
    if stmt.order_by_columns:
        cols = ", ".join(render_state(col, ctx) for col in stmt.order_by_columns)
        parts.append(f"ORDER BY {cols}")

    # LIMIT/OFFSET clause
    if stmt.limit:
        parts.append(f"LIMIT {stmt.limit.count}")
        if stmt.limit.offset:
            parts.append(f"OFFSET {stmt.limit.offset}")

    return " ".join(parts)


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
    return ctx.add_param(param.name, param.value)


def render_function(func: Function, ctx: RenderContext) -> str:
    """Render a Function to SQL.

    Args:
        func: A Function to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    if func.args:
        if func.name == "COUNT(DISTINCT":
            # COUNT(DISTINCT expr) - special name stores the opening
            args_sql = ", ".join(render_state(arg, ctx) for arg in func.args)
            sql = f"COUNT(DISTINCT {args_sql})"
        else:
            # Integer literals (e.g. NTILE(4), LAG offset) render as-is
            rendered_args = []
            for arg in func.args:
                if isinstance(arg, int):
                    rendered_args.append(str(arg))
                else:
                    rendered_args.append(render_state(arg, ctx))
            sql = f"{func.name}({', '.join(rendered_args)})"
    else:
        # COUNT(*) and COUNT(DISTINCT *) store the full expression in the name
        if func.name in ("COUNT(*)", "COUNT(DISTINCT *)"):
            sql = func.name
        else:
            sql = f"{func.name}()"

    if func.filter:
        sql += f" FILTER (WHERE {render_state(func.filter, ctx)})"

    return sql


def render_window_function(wf: WindowFunction, ctx: RenderContext) -> str:
    """Render a WindowFunction to SQL.

    Args:
        wf: A WindowFunction to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    over_parts = []

    if wf.partition_by:
        cols = ", ".join(render_state(col, ctx) for col in wf.partition_by)
        over_parts.append(f"PARTITION BY {cols}")

    if wf.order_by:
        cols = ", ".join(render_state(col, ctx) for col in wf.order_by)
        over_parts.append(f"ORDER BY {cols}")

    if wf.frame:
        over_parts.append(render_state(wf.frame, ctx))

    return f"{render_state(wf.function, ctx)} OVER ({' '.join(over_parts)})"


def render_frame_clause(frame: FrameClause, ctx: RenderContext) -> str:
    """Render a FrameClause to SQL.

    Args:
        frame: A FrameClause to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    sql = f"{frame.mode} BETWEEN {render_state(frame.start, ctx)} AND {render_state(frame.end, ctx)}"
    if frame.exclude:
        sql += f" EXCLUDE {frame.exclude}"
    return sql


def render_join(join: Join, ctx: RenderContext) -> str:
    """Render a Join to SQL.

    Args:
        join: A Join to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "INNER JOIN orders AS o ON (u.id = o.user_id)").
    """
    parts = [f"{join.jtype.value} JOIN {render_source(join.right, ctx)}"]

    if join.on:
        on_sql = " AND ".join(render_state(cond, ctx) for cond in join.on)
        parts.append(f"ON ({on_sql})")

    if join.using:
        using_sql = ", ".join(render_state(col, ctx) for col in join.using)
        parts.append(f"USING ({using_sql})")

    return " ".join(parts)


def render_exists(exists: Exists, ctx: RenderContext) -> str:
    """Render EXISTS subquery check to SQL.

    Args:
        exists: An Exists state to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "EXISTS (SELECT ...)").
    """
    # Reference needs wrapping as a subquery; Statement/SetOperation render directly
    if isinstance(exists.subquery, Reference):
        subquery_sql = f"SELECT * FROM {render_state(exists.subquery, ctx)}"
    else:
        subquery_sql = render_state(exists.subquery, ctx)
    return f"EXISTS ({subquery_sql})"


def render_set_operation(setop: SetOperation, ctx: RenderContext) -> str:
    """Render a set operation to SQL.

    Args:
        setop: A SetOperation to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string (e.g., "(SELECT ...) UNION (SELECT ...)").
    """
    # Reference sides need wrapping as SELECT *; Statement/SetOperation render directly
    if isinstance(setop.left, Reference):
        left_sql = f"SELECT * FROM {render_state(setop.left, ctx)}"
    else:
        left_sql = render_state(setop.left, ctx)
    if isinstance(setop.right, Reference):
        right_sql = f"SELECT * FROM {render_state(setop.right, ctx)}"
    else:
        right_sql = render_state(setop.right, ctx)
    return f"({left_sql}) {setop.operator} ({right_sql})"


def render_with_clause(ctx: RenderContext) -> str:
    """Render the WITH clause from registered CTEs.

    Args:
        ctx: Rendering context with registered CTEs.

    Returns:
        The WITH clause SQL (e.g., "WITH RECURSIVE cte1 AS (...), cte2 AS (...)").
    """
    if any(cte.recursive for cte in ctx.ctes):
        with_keyword = "WITH RECURSIVE"
    else:
        with_keyword = "WITH"
    cte_definitions = ", ".join(f"{cte.name} AS ({cte.body_sql})" for cte in ctx.ctes)
    return f"{with_keyword} {cte_definitions}"
