"""VALUES clause for inline row data.

This module provides a VALUES implementation that can be used as a row source
in FROM clauses, JOINs, CTEs, and as part of INSERT statements.

Example:
    >>> from vw import values, col, Source
    >>>
    >>> # Standalone VALUES as a row source
    >>> v = values("users", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
    >>> result = v.select(col("*")).render()
    >>>
    >>> # VALUES in a JOIN
    >>> ids = values("ids", {"id": 1}, {"id": 2})
    >>> Source("users").join.inner(ids, on=[col("users.id") == col("ids.id")])
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from vw.base import RowSet
from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Values(RowSet):
    """Represents a VALUES clause as a row source.

    VALUES can be used anywhere a table/rowset is expected: FROM clauses,
    JOINs, subqueries, and CTEs.

    Example:
        >>> values("t", {"id": 1, "name": "Alice"}).select(col("*"))
    """

    rows: tuple[dict[str, Any], ...]

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the VALUES clause."""
        if not self.rows:
            raise ValueError("VALUES requires at least one row")

        if not self._alias:
            raise ValueError("VALUES requires an alias")

        columns = list(self.rows[0].keys())

        row_sqls = []
        for row_idx, row in enumerate(self.rows):
            placeholders = []
            for col_idx, col_name in enumerate(columns):
                value = row[col_name]
                if hasattr(value, "__vw_render__"):
                    # It's an Expression - render it directly
                    placeholders.append(value.__vw_render__(context))
                else:
                    # Raw Python value - parameterize it
                    param_name = f"_v{row_idx}_{col_idx}_{col_name}"
                    placeholder = context.add_param(param_name, value)
                    placeholders.append(placeholder)
            row_sqls.append(f"({', '.join(placeholders)})")

        values_sql = f"VALUES {', '.join(row_sqls)}"

        # Wrap in subquery with column names
        col_list = ", ".join(columns)
        return f"({values_sql}) AS {self._alias}({col_list})"


def values(alias: str, *rows: dict[str, Any]) -> Values:
    """Create a VALUES clause from row dictionaries.

    Args:
        alias: Required alias for the VALUES clause.
        *rows: Row dictionaries where keys are column names and values
               are the data. All rows must have the same keys.

    Returns:
        A Values object that can be used as a row source.

    Example:
        >>> v = values("users", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
        >>> result = v.select(col("*")).render()
        # SELECT * FROM (VALUES ($_v0_0_id, $_v0_1_name), ...) AS users(id, name)
    """
    return Values(rows=rows, _alias=alias)


__all__ = [
    "Values",
    "values",
]
