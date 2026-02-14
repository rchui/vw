"""DuckDB SQL rendering."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vw.core.render import SQL, ParamStyle, RenderConfig, RenderContext

if TYPE_CHECKING:
    from vw.duckdb.base import Expression

from vw.core.states import (
    CTE,
    Alias,
    Asc,
    Between,
    Case,
    Cast,
    Column,
    Cube,
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
    GroupingSets,
    IsIn,
    IsNotIn,
    IsNotNull,
    IsNull,
    Join,
    Like,
    Literal,
    Not,
    NotBetween,
    NotLike,
    Operator,
    Parameter,
    Preceding,
    RawExpr,
    RawSource,
    Reference,
    Rollup,
    ScalarSubquery,
    SetOperation,
    Statement,
    UnboundedFollowing,
    UnboundedPreceding,
    Values,
    WindowFunction,
)
from vw.duckdb.base import RowSet
from vw.duckdb.states import Star


def render(obj: RowSet | Expression, *, config: RenderConfig | None = None) -> SQL:
    """Render a RowSet or Expression to DuckDB SQL.

    Args:
        obj: A RowSet or Expression to render.
        config: Optional rendering configuration (defaults to DuckDB DOLLAR style).

    Returns:
        SQL result with query string and parameters dict.

    Example:
        >>> from vw.duckdb import ref, col, param, render
        >>> result = render(ref("users").select(col("id")).where(col("age") >= param("min_age", 18)))
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
        case Star():
            return render_star(state, ctx)
        case Parameter():
            return render_parameter(state, ctx)
        case Literal():
            return render_literal(state, ctx)

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
            # DuckDB uses :: syntax for casting (same as PostgreSQL)
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

        # --- Grouping Constructs --------------------------------------- #
        case Rollup():
            cols = ", ".join(render_state(c, ctx) for c in state.columns)
            return f"ROLLUP ({cols})"
        case Cube():
            cols = ", ".join(render_state(c, ctx) for c in state.columns)
            return f"CUBE ({cols})"
        case GroupingSets():
            rendered = []
            for group in state.sets:
                if not group:
                    rendered.append("()")
                else:
                    rendered.append(f"({', '.join(render_state(e, ctx) for e in group)})")
            return f"GROUPING SETS ({', '.join(rendered)})"

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

        # --- Raw SQL Escape Hatches ------------------------------------ #
        case RawExpr():
            return render_raw_expr(state, ctx)
        case RawSource():
            return render_raw_source(state, ctx)

        case _:
            raise TypeError(f"Unknown state type: {type(state)}")


def render_source(source: CTE | Statement | SetOperation | Reference | Values | RawSource, ctx: RenderContext) -> str:
    """Render a source (CTE, Statement subquery, SetOperation, Reference, Values, or RawSource) to SQL.

    For CTEs: registers the CTE body and returns the CTE name reference.
    For Statements/SetOperations: wraps in parens as a subquery.
    For References: renders the table/view name.
    For Values: renders the VALUES clause with alias and column list.
    For RawSource: renders raw SQL with parameter substitution.
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
    elif isinstance(source, RawSource):
        return render_state(source, ctx)
    else:
        # Reference: render name, modifiers, then alias
        parts = [source.name]

        # Modifiers between name and alias (e.g., TABLESAMPLE)
        for modifier in source.modifiers:
            parts.append(render_state(modifier, ctx))

        if source.alias:
            parts.append(f"AS {source.alias}")

        return " ".join(parts)


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

    # Build result with modifiers between VALUES and alias
    parts = [f"({values_sql})"]

    # Modifiers (rare for VALUES but supported for consistency)
    for modifier in values_src.modifiers:
        parts.append(render_state(modifier, ctx))

    # Alias and column list
    parts.append(f"AS {values_src.alias}({col_list})")

    return " ".join(parts)


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

    # SELECT clause (with optional DISTINCT)
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

    # LIMIT clause
    if stmt.limit:
        parts.append(f"LIMIT {stmt.limit.count}")

    # OFFSET clause
    if stmt.offset is not None:
        parts.append(f"OFFSET {stmt.offset}")

    # FETCH clause
    if stmt.fetch:
        ties = "WITH TIES" if stmt.fetch.with_ties else "ONLY"
        parts.append(f"FETCH FIRST {stmt.fetch.count} ROWS {ties}")

    # Modifiers - rendered at the end
    for modifier in stmt.modifiers:
        parts.append(render_state(modifier, ctx))

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


def render_star(star: Star, ctx: RenderContext) -> str:
    """Render a DuckDB Star expression with optional EXCLUDE/REPLACE.

    Args:
        star: A Star to render.
        ctx: Rendering context.

    Returns:
        The SQL string with DuckDB extensions.

    Raises:
        TypeError: If the source type or modifier type is unsupported.
    """
    from vw.duckdb.states import StarExclude, StarReplace

    # Base star
    if star.source:
        # Extract the name to qualify the star
        # Note: Check CTE before Statement since CTE is a subclass of Statement
        if isinstance(star.source, Reference):
            source_name = star.source.alias if star.source.alias else star.source.name
        elif isinstance(star.source, CTE):
            source_name = star.source.name
        elif isinstance(star.source, Statement):
            source_name = star.source.alias
        else:
            msg = f"Unsupported Star source type: {type(star.source).__name__}"
            raise TypeError(msg)

        base = f"{source_name}.*"
    else:
        base = "*"

    # Apply modifiers in order
    parts = [base]

    for modifier in star.modifiers:
        if isinstance(modifier, StarExclude):
            excluded = ", ".join(render_state(e, ctx) for e in modifier.columns)
            parts.append(f"EXCLUDE ({excluded})")
        elif isinstance(modifier, StarReplace):
            replacements = ", ".join(
                f"{render_state(expr, ctx)} AS {name}" for name, expr in modifier.replacements.items()
            )
            parts.append(f"REPLACE ({replacements})")
        else:
            msg = f"Unsupported Star modifier type: {type(modifier).__name__}"
            raise TypeError(msg)

    result = " ".join(parts)

    if star.alias:
        return f"{result} AS {star.alias}"
    return result


def render_parameter(param: Parameter, ctx: RenderContext) -> str:
    """Render a Parameter to SQL using parameter binding.

    Args:
        param: A Parameter to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL placeholder string (e.g., "$age" for DuckDB).
    """
    return ctx.add_param(param.name, param.value)


def render_literal(lit: Literal, ctx: RenderContext) -> str:
    """Render a Literal directly in SQL with proper escaping.

    Args:
        lit: A Literal to render.
        ctx: Rendering context (unused, kept for signature consistency).

    Returns:
        The SQL literal string.
    """

    if lit.value is None:
        return "NULL"
    elif isinstance(lit.value, bool):
        return "TRUE" if lit.value else "FALSE"
    elif isinstance(lit.value, str):
        # SQL standard: escape single quotes by doubling them
        escaped = lit.value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(lit.value, (int, float)):
        # int, float, or other numeric types
        return str(lit.value)
    else:
        context = {"type": type(lit.value), "value": lit.value}
        raise ValueError(f"Unsupported literal: {context}")


def render_function(func: Function, ctx: RenderContext) -> str:
    """Render a Function to SQL.

    Args:
        func: A Function to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    if func.args:
        # Render arguments
        rendered_args = [render_state(arg, ctx) for arg in func.args]
        args_str = ", ".join(rendered_args)

        # Handle DISTINCT
        if func.distinct:
            sql = f"{func.name}(DISTINCT {args_str}"
        else:
            sql = f"{func.name}({args_str}"

        # Add ORDER BY if present (before closing paren)
        if func.order_by:
            order_cols = ", ".join(render_state(col, ctx) for col in func.order_by)
            sql += f" ORDER BY {order_cols}"

        sql += ")"
    else:
        # No args
        sql = f"{func.name}()"

    # Add FILTER clause if present
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

    When start or end is None, applies appropriate defaults:
    - start defaults to UNBOUNDED PRECEDING
    - end defaults to CURRENT ROW

    Args:
        frame: A FrameClause to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string.
    """
    # Apply defaults when None
    if frame.start is None:
        start_sql = "UNBOUNDED PRECEDING"
    else:
        start_sql = render_state(frame.start, ctx)

    if frame.end is None:
        end_sql = "CURRENT ROW"
    else:
        end_sql = render_state(frame.end, ctx)

    sql = f"{frame.mode} BETWEEN {start_sql} AND {end_sql}"
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
    right_sql = render_source(join.right, ctx)

    # Handle LATERAL keyword (DuckDB supports LATERAL)
    if join.lateral:
        right_sql = f"LATERAL {right_sql}"

    parts = [f"{join.jtype} JOIN {right_sql}"]

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


def render_raw_expr(raw_expr: RawExpr, ctx: RenderContext) -> str:
    """Render a RawExpr to SQL with named parameter substitution.

    Args:
        raw_expr: A RawExpr to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string with placeholders substituted.

    Example:
        RawExpr(sql="{x} @> {y}", params=(("x", Column("tags")), ("y", Parameter("tag", "python"))))
        renders to: tags @> $tag
    """
    sql = raw_expr.sql
    for name, expr_state in raw_expr.params:
        rendered = render_state(expr_state, ctx)
        sql = sql.replace(f"{{{name}}}", rendered)
    return sql


def render_raw_source(raw_source: RawSource, ctx: RenderContext) -> str:
    """Render a RawSource to SQL with named parameter substitution.

    Args:
        raw_source: A RawSource to render.
        ctx: Rendering context for parameter collection.

    Returns:
        The SQL string with placeholders substituted and optional alias.

    Example:
        RawSource(sql="generate_series(1, {n})", params=(("n", Parameter("max", 10)),), alias="t")
        renders to: generate_series(1, $max) AS t
    """
    sql = raw_source.sql
    for name, expr_state in raw_source.params:
        rendered = render_state(expr_state, ctx)
        sql = sql.replace(f"{{{name}}}", rendered)
    # Add alias if present (note: alias might already be in template)
    if raw_source.alias:
        return f"{sql} AS {raw_source.alias}"
    return sql
