"""PostgreSQL SQL rendering."""

from __future__ import annotations

from vw.core.states import Column, Source, Statement
from vw.postgres.base import Expression, RowSet


def render(obj: RowSet | Expression) -> str:
    """Render a RowSet or Expression to PostgreSQL SQL.

    Args:
        obj: A RowSet or Expression to render.

    Returns:
        The SQL string.

    Example:
        >>> from vw.postgres import source, col, render
        >>> render(source("users"))
        'FROM users'
        >>> render(source("users").select(col("id")))
        'SELECT id FROM users'
    """
    # Top-level Source should render with FROM
    if isinstance(obj.state, Source):
        return f"FROM {render_source(obj.state)}"
    return render_state(obj.state)


def render_state(state: object) -> str:
    """Render a state object to SQL.

    Args:
        state: A state object (Source, Statement, Column, etc.).

    Returns:
        The SQL string.

    Raises:
        TypeError: If the state type is unknown.
    """
    if isinstance(state, Statement):
        return render_statement(state)
    elif isinstance(state, Source):
        return render_source(state)
    elif isinstance(state, Column):
        return render_column(state)
    else:
        raise TypeError(f"Unknown state type: {type(state)}")


def render_statement(stmt: Statement) -> str:
    """Render a Statement to SQL.

    Args:
        stmt: A Statement to render.

    Returns:
        The SQL string.
    """
    parts = []

    # SELECT clause (with optional DISTINCT)
    if stmt.columns:
        select_clause = "SELECT"
        if stmt.distinct:
            select_clause = "SELECT DISTINCT"
        cols = ", ".join(render_state(col.state) for col in stmt.columns)
        parts.append(f"{select_clause} {cols}")

    # FROM clause (source can be Source or Statement for subqueries)
    if isinstance(stmt.source, Source):
        source_sql = render_source(stmt.source)
    else:  # Statement (subquery)
        source_sql = f"({render_statement(stmt.source)})"
        if stmt.source.alias:
            source_sql += f" AS {stmt.source.alias}"
    parts.append(f"FROM {source_sql}")

    # WHERE clause
    if stmt.where_conditions:
        conditions = " AND ".join(render_state(cond.state) for cond in stmt.where_conditions)
        parts.append(f"WHERE {conditions}")

    # GROUP BY clause
    if stmt.group_by_columns:
        cols = ", ".join(render_state(col.state) for col in stmt.group_by_columns)
        parts.append(f"GROUP BY {cols}")

    # HAVING clause
    if stmt.having_conditions:
        conditions = " AND ".join(render_state(cond.state) for cond in stmt.having_conditions)
        parts.append(f"HAVING {conditions}")

    # ORDER BY clause
    if stmt.order_by_columns:
        cols = ", ".join(render_state(col.state) for col in stmt.order_by_columns)
        parts.append(f"ORDER BY {cols}")

    # LIMIT/OFFSET clause
    if stmt.limit:
        parts.append(f"LIMIT {stmt.limit.count}")
        if stmt.limit.offset:
            parts.append(f"OFFSET {stmt.limit.offset}")

    return " ".join(parts)


def render_source(source: Source) -> str:
    """Render a Source to SQL.

    Args:
        source: A Source to render.

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
