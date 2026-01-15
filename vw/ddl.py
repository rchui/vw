"""DDL (Data Definition Language) classes for CREATE TABLE.

This module provides a minimal CREATE TABLE implementation focused on
CTAS (CREATE TABLE AS SELECT) and simple schema definitions.

Example:
    >>> from vw import Source, col
    >>> from vw import dtypes
    >>>
    >>> # CREATE TABLE AS SELECT
    >>> stmt = Source(name="backup").create_table().as_select(
    ...     Source(name="orders").select(col("*")).where(col("year") == col("2024"))
    ... )
    >>>
    >>> # CREATE TABLE with schema
    >>> stmt = Source(name="users").create_table({
    ...     "id": dtypes.integer(),
    ...     "name": dtypes.varchar(100),
    ...     "balance": dtypes.decimal(10, 2),
    ... })
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from vw.render import Dialect, RenderConfig, RenderContext, RenderResult

if TYPE_CHECKING:
    from vw.build import Statement


@dataclass(kw_only=True, frozen=True)
class CreateTable:
    """Represents a CREATE TABLE statement.

    Created via Source("name").create_table().

    Example:
        >>> # CTAS
        >>> Source("backup").create_table().as_select(query)
        >>> Source("temp").create_table().as_select(query).temporary()
        >>>
        >>> # With schema
        >>> Source("users").create_table({
        ...     "id": dtypes.integer(),
        ...     "name": dtypes.varchar(100),
        ... })
    """

    name: str
    schema: dict[str, str] = field(default_factory=dict)
    _as_select: Statement | None = None
    _temporary: bool = False
    _if_not_exists: bool = False
    _or_replace: bool = False

    def as_select(self, query: Statement) -> CreateTable:
        """Set the SELECT query for CREATE TABLE AS SELECT.

        Args:
            query: The SELECT statement to create the table from.

        Returns:
            A new CreateTable with the AS SELECT clause.

        Example:
            >>> Source("backup").create_table().as_select(
            ...     Source("orders").select(col("*"))
            ... )
        """
        return replace(self, _as_select=query)

    def temporary(self) -> CreateTable:
        """Mark table as TEMPORARY.

        Returns:
            A new CreateTable with TEMPORARY modifier.

        Example:
            >>> Source("temp_data").create_table().as_select(query).temporary()
        """
        return replace(self, _temporary=True)

    def if_not_exists(self) -> CreateTable:
        """Add IF NOT EXISTS clause.

        Returns:
            A new CreateTable with IF NOT EXISTS modifier.

        Example:
            >>> Source("cache").create_table({...}).if_not_exists()
        """
        return replace(self, _if_not_exists=True)

    def or_replace(self) -> CreateTable:
        """Add OR REPLACE clause (DuckDB/Snowflake only).

        Returns:
            A new CreateTable with OR REPLACE modifier.

        Example:
            >>> Source("snapshot").create_table().as_select(query).or_replace()
        """
        return replace(self, _or_replace=True)

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """Render the CREATE TABLE statement.

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
        if context.ctes:
            cte_definitions = [f"{cte.name} AS {body_sql}" for cte, body_sql in context.ctes]
            sql = f"WITH {', '.join(cte_definitions)} {sql}"

        return RenderResult(sql=sql, params=context.params)

    def __vw_render__(self, context: RenderContext) -> str:
        """Render the CREATE TABLE statement."""
        # SQL Server uses SELECT INTO for CTAS - not yet supported
        if self._as_select is not None and context.config.dialect == Dialect.SQLSERVER:
            raise NotImplementedError(
                "CREATE TABLE AS SELECT is not yet supported for SQL Server. "
                "SQL Server uses SELECT INTO syntax which requires different rendering."
            )

        parts = ["CREATE"]

        if self._or_replace:
            parts.append("OR REPLACE")
        if self._temporary:
            parts.append("TEMPORARY")

        parts.append("TABLE")

        if self._if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.name)

        if self._as_select is not None:
            # CREATE TABLE AS SELECT
            parts.append("AS")
            parts.append(self._as_select.__vw_render__(context))
        elif self.schema:
            # CREATE TABLE with schema
            col_defs = [f"{name} {dtype}" for name, dtype in self.schema.items()]
            parts.append(f"({', '.join(col_defs)})")
        else:
            # Empty table
            parts.append("()")

        return " ".join(parts)


__all__ = [
    "CreateTable",
]
