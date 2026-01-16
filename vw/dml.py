"""DML (Data Manipulation Language) classes for INSERT, UPDATE, DELETE.

This module provides INSERT statement implementation.

Example:
    >>> from vw import Source, values, col
    >>>
    >>> # INSERT with VALUES
    >>> Source("users").insert(values({"name": "Alice", "age": 30}))
    >>>
    >>> # INSERT from SELECT
    >>> Source("users_backup").insert(
    ...     Source("users").select(col("*")).where(col("active") == col("true"))
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from vw.base import Expression
from vw.render import RenderConfig, RenderContext, RenderResult
from vw.values import Values, render_values_rows

if TYPE_CHECKING:
    from vw.build import Statement


@dataclass(kw_only=True, frozen=True)
class Insert:
    """Represents an INSERT statement.

    Created via Source("name").insert(source).

    Example:
        >>> # INSERT with VALUES
        >>> Source("users").insert(values({"name": "Alice"}))
        >>>
        >>> # INSERT from SELECT
        >>> Source("users_backup").insert(query)
        >>>
        >>> # With RETURNING
        >>> Source("users").insert(values({"name": "Alice"})).returning(col("id"))
    """

    table: str
    source: Values | Statement
    _returning: tuple[Expression, ...] = field(default_factory=tuple)

    def returning(self, *exprs: Expression) -> Insert:
        """Add RETURNING clause (PostgreSQL/DuckDB).

        Args:
            *exprs: Expressions to return from the inserted rows.

        Returns:
            A new Insert with RETURNING clause.

        Example:
            >>> Source("users").insert(values({"name": "Alice"})).returning(col("id"))
            >>> Source("users").insert(values({"name": "Alice"})).returning(col("*"))
        """
        return replace(self, _returning=self._returning + exprs)

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """Render the INSERT statement.

        Args:
            config: Rendering configuration.

        Returns:
            RenderResult containing the SQL string and parameter dictionary.
        """
        if config is None:
            config = RenderConfig()
        context = RenderContext(config=config)
        sql = self.__vw_render__(context)

        # Prepend WITH clause if CTEs were registered (for INSERT ... SELECT with CTEs)
        with_clause = context.render_ctes()
        if with_clause:
            sql = f"{with_clause} {sql}"

        return RenderResult(sql=sql, params=context.params)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the INSERT statement."""
        if isinstance(self.source, Values):
            # INSERT with VALUES
            columns, values_sql = render_values_rows(self.source, context)
            col_list = ", ".join(columns)
            sql = f"INSERT INTO {self.table} ({col_list}) {values_sql}"
        else:
            # INSERT from SELECT
            select_sql = self.source.__vw_render__(context)
            sql = f"INSERT INTO {self.table} {select_sql}"

        if self._returning:
            returning_cols = ", ".join(expr.__vw_render__(context) for expr in self._returning)
            sql += f" RETURNING {returning_cols}"

        return sql


__all__ = [
    "Insert",
]
