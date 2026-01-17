"""DML (Data Manipulation Language) classes for INSERT, UPDATE, DELETE.

This module provides INSERT and DELETE statement implementations.

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
    >>>
    >>> # DELETE with WHERE
    >>> Source("users").delete().where(col("id") == param("id", 1))
    >>>
    >>> # DELETE with USING
    >>> Source("users").delete(Source("orders").alias("o")).where(
    ...     col("users.id") == col("o.user_id")
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from vw.base import Expression, RowSet
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


@dataclass(kw_only=True, frozen=True)
class Delete:
    """Represents a DELETE statement.

    Created via Source("name").delete() or Source("name").delete(using).

    Example:
        >>> # Basic DELETE
        >>> Source("users").delete().where(col("id") == param("id", 1))
        >>>
        >>> # DELETE with USING
        >>> Source("users").delete(Source("orders").alias("o")).where(
        ...     col("users.id") == col("o.user_id")
        ... )
        >>>
        >>> # DELETE with RETURNING
        >>> Source("users").delete().where(...).returning(col("*"))
    """

    table: str
    _using: RowSet | None = None
    _where: tuple[Expression, ...] = field(default_factory=tuple)
    _returning: tuple[Expression, ...] = field(default_factory=tuple)

    def where(self, *exprs: Expression) -> Delete:
        """Add WHERE conditions to the DELETE statement.

        Args:
            *exprs: Expressions for WHERE clause. Multiple are combined with AND.

        Returns:
            A new Delete with WHERE conditions.

        Example:
            >>> Source("users").delete().where(col("id") == param("id", 1))
            >>> Source("users").delete().where(col("active") == col("false"), col("age") < col("18"))
        """
        return replace(self, _where=self._where + exprs)

    def returning(self, *exprs: Expression) -> Delete:
        """Add RETURNING clause (PostgreSQL/DuckDB).

        Args:
            *exprs: Expressions to return from the deleted rows.

        Returns:
            A new Delete with RETURNING clause.

        Example:
            >>> Source("users").delete().where(...).returning(col("id"))
            >>> Source("users").delete().where(...).returning(col("*"))
        """
        return replace(self, _returning=self._returning + exprs)

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """Render the DELETE statement.

        Args:
            config: Rendering configuration.

        Returns:
            RenderResult containing the SQL string and parameter dictionary.
        """
        if config is None:
            config = RenderConfig()
        context = RenderContext(config=config)
        sql = self.__vw_render__(context)

        # Prepend WITH clause if CTEs were registered
        with_clause = context.render_ctes()
        if with_clause:
            sql = f"{with_clause} {sql}"

        return RenderResult(sql=sql, params=context.params)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the DELETE statement."""
        sql = f"DELETE FROM {self.table}"

        if self._using is not None:
            using_sql = self._using.__vw_render__(context.recurse())
            sql += f" USING {using_sql}"

        if self._where:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self._where]
            sql += f" WHERE {' AND '.join(conditions)}"

        if self._returning:
            returning_cols = ", ".join(expr.__vw_render__(context) for expr in self._returning)
            sql += f" RETURNING {returning_cols}"

        return sql


@dataclass(kw_only=True, frozen=True)
class Update:
    """Represents an UPDATE statement.

    Created via Source("name").update() or Source("name").update(using=...).

    Example:
        >>> # Basic UPDATE
        >>> Source("users").update().set({"name": param("name", "Alice")}).where(
        ...     col("id") == param("id", 1)
        ... )
        >>>
        >>> # UPDATE with USING (renders as FROM in SQL)
        >>> Source("users").update(using=Source("orders").alias("o")).set({
        ...     "total": col("o.amount")
        ... }).where(col("users.id") == col("o.user_id"))
        >>>
        >>> # UPDATE with RETURNING
        >>> Source("users").update().set({"active": col("false")}).where(...).returning(col("*"))
    """

    table: str
    _using: RowSet | None = None
    _set: tuple[tuple[str, Expression], ...] = field(default_factory=tuple)
    _where: tuple[Expression, ...] = field(default_factory=tuple)
    _returning: tuple[Expression, ...] = field(default_factory=tuple)

    def set(self, assignments: dict[str, Expression]) -> Update:
        """Add SET assignments to the UPDATE statement.

        Args:
            assignments: Dictionary mapping column names to expressions.

        Returns:
            A new Update with SET assignments.

        Example:
            >>> Source("users").update().set({"name": param("name", "Alice")})
            >>> Source("users").update().set({
            ...     "name": param("name", "Alice"),
            ...     "updated_at": col("NOW()")
            ... })
        """
        new_assignments = tuple(assignments.items())
        return replace(self, _set=self._set + new_assignments)

    def where(self, *exprs: Expression) -> Update:
        """Add WHERE conditions to the UPDATE statement.

        Args:
            *exprs: Expressions for WHERE clause. Multiple are combined with AND.

        Returns:
            A new Update with WHERE conditions.

        Example:
            >>> Source("users").update().set({...}).where(col("id") == param("id", 1))
            >>> Source("users").update().set({...}).where(
            ...     col("active") == col("true"),
            ...     col("age") >= col("18")
            ... )
        """
        return replace(self, _where=self._where + exprs)

    def returning(self, *exprs: Expression) -> Update:
        """Add RETURNING clause (PostgreSQL/SQLite 3.35+).

        Args:
            *exprs: Expressions to return from the updated rows.

        Returns:
            A new Update with RETURNING clause.

        Example:
            >>> Source("users").update().set({...}).where(...).returning(col("id"))
            >>> Source("users").update().set({...}).where(...).returning(col("*"))
        """
        return replace(self, _returning=self._returning + exprs)

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """Render the UPDATE statement.

        Args:
            config: Rendering configuration.

        Returns:
            RenderResult containing the SQL string and parameter dictionary.
        """
        if config is None:
            config = RenderConfig()
        context = RenderContext(config=config)
        sql = self.__vw_render__(context)

        # Prepend WITH clause if CTEs were registered
        with_clause = context.render_ctes()
        if with_clause:
            sql = f"{with_clause} {sql}"

        return RenderResult(sql=sql, params=context.params)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the UPDATE statement."""
        if not self._set:
            raise ValueError("UPDATE statement requires at least one SET assignment")

        sql = f"UPDATE {self.table}"

        # SET clause
        assignments = [f"{col} = {expr.__vw_render__(context)}" for col, expr in self._set]
        sql += f" SET {', '.join(assignments)}"

        # FROM clause (via _using, PostgreSQL-style joins)
        if self._using is not None:
            using_sql = self._using.__vw_render__(context.recurse())
            sql += f" FROM {using_sql}"

        # WHERE clause
        if self._where:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self._where]
            sql += f" WHERE {' AND '.join(conditions)}"

        # RETURNING clause
        if self._returning:
            returning_cols = ", ".join(expr.__vw_render__(context) for expr in self._returning)
            sql += f" RETURNING {returning_cols}"

        return sql


__all__ = [
    "Delete",
    "Insert",
    "Update",
]
