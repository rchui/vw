"""VALUES clause for inline row data.

This module provides a VALUES implementation that can be used as a row source
in FROM clauses, JOINs, CTEs, and as part of INSERT statements.

Example:
    >>> from vw import values, col, Source
    >>>
    >>> # Standalone VALUES as a row source (requires alias)
    >>> v = values({"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}).alias("users")
    >>> result = v.select(col("*")).render()
    >>>
    >>> # VALUES in a JOIN
    >>> ids = values({"id": 1}, {"id": 2}).alias("ids")
    >>> Source("users").join.inner(ids, on=[col("users.id") == col("ids.id")])
    >>>
    >>> # VALUES for INSERT (no alias needed)
    >>> Source("users").insert(values({"id": 1, "name": "Alice"}))
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from vw.base import RowSet
from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Values(RowSet):
    """Represents a VALUES clause as a row source.

    VALUES can be used anywhere a table/rowset is expected: FROM clauses,
    JOINs, subqueries, CTEs, and INSERT statements.

    Example:
        >>> values({"id": 1, "name": "Alice"}).alias("t").select(col("*"))
        >>> Source("users").insert(values({"name": "Alice"}))
    """

    rows: tuple[dict[str, Any], ...]

    def alias(self, name: str) -> Values:
        """Set an alias for this VALUES clause.

        Required when using VALUES in FROM, JOIN, subquery, or CTE contexts.
        Not needed for INSERT statements.

        Args:
            name: The alias name.

        Returns:
            A new Values with the alias set.

        Example:
            >>> values({"id": 1}).alias("v")
        """
        return replace(self, _alias=name)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the VALUES clause for use as a row source."""
        if not self._alias:
            raise ValueError(
                "VALUES requires an alias when used as a row source. "
                "Use .alias('name') to set one."
            )

        columns, values_sql = render_values_rows(self, context)
        col_list = ", ".join(columns)
        return f"({values_sql}) AS {self._alias}({col_list})"


def render_values_rows(
    vals: Values, context: RenderContext
) -> tuple[list[str], str]:
    """Render Values as a VALUES clause.

    Args:
        vals: The Values object containing rows.
        context: The render context for parameterization.

    Returns:
        A tuple of (column_names, values_sql).

    Example:
        >>> columns, sql = render_values_rows(v, context)
        >>> # columns = ["id"], sql = "VALUES ($1)"
    """
    if not vals.rows:
        raise ValueError("VALUES requires at least one row")

    columns = list(vals.rows[0].keys())

    row_sqls = []
    for row_idx, row in enumerate(vals.rows):
        placeholders = []
        for col_idx, col_name in enumerate(columns):
            value = row[col_name]
            if hasattr(value, "__vw_render__"):
                placeholders.append(value.__vw_render__(context))
            else:
                param_name = f"_v{row_idx}_{col_idx}_{col_name}"
                placeholder = context.add_param(param_name, value)
                placeholders.append(placeholder)
        row_sqls.append(f"({', '.join(placeholders)})")

    values_sql = f"VALUES {', '.join(row_sqls)}"
    return columns, values_sql


def values(*rows: dict[str, Any]) -> Values:
    """Create a VALUES clause from row dictionaries.

    Args:
        *rows: Row dictionaries where keys are column names and values
               are the data. All rows must have the same keys.

    Returns:
        A Values object that can be used as a row source.

    Example:
        >>> # For FROM/JOIN (requires alias)
        >>> v = values({"id": 1, "name": "Alice"}).alias("users")
        >>> result = v.select(col("*")).render()
        >>>
        >>> # For INSERT (no alias needed)
        >>> Source("users").insert(values({"name": "Alice"}))
    """
    return Values(rows=rows, _alias=None)


__all__ = [
    "Values",
    "render_values_rows",
    "values",
]
