"""SQL query builder with method chaining."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from vw.expr import Column, Expression
from vw.render import RenderConfig, RenderContext, RenderResult


@dataclass
class InnerJoin:
    """Represents an INNER JOIN operation."""

    right: Source
    on: Sequence[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the inner join."""

        join_sql = f"INNER JOIN {self.right.__vw_render__(context)}"
        if self.on:
            conditions = [expr.__vw_render__(context) for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        return join_sql


class JoinAccessor:
    """Accessor for join operations on a Source."""

    def __init__(self, source: Source):
        self._source = source

    def inner(self, right: Source, *, on: Sequence[Expression] = ()) -> Source:
        """
        Perform an INNER JOIN with another source.

        Args:
            right: The source to join with.
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new Source with the join applied.

        Example:
            >>> users = Source("users")
            >>> orders = Source("orders")
            >>> users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return Source(
            name=self._source.name,
            joins=self._source.joins + [InnerJoin(right=right, on=on)],
        )


@dataclass
class Source:
    """Represents a SQL data source (table, view, etc.)."""

    name: str
    joins: list[InnerJoin] = field(default_factory=list)

    @property
    def join(self) -> JoinAccessor:
        """Access join operations."""
        return JoinAccessor(self)

    def col(self, column_name: str, /) -> Column:
        """
        Create a column reference qualified with this source's name.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the source name as a prefix.

        Example:
            >>> Source("users").col("id")  # Returns Column("users.id")
        """

        return Column(f"{self.name}.{column_name}")

    def select(self, *columns: Expression) -> Statement:
        """
        Select columns from this source.

        Args:
            *columns: Expression objects to select.

        Returns:
            A Statement object for method chaining.

        Example:
            >>> from vw import col
            >>> Source("users").select(col("*"))
            >>> Source("users").select(col("id"), col("name"))
        """

        return Statement(source=self, columns=list(columns))

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the source."""

        sql = self.name
        if self.joins:
            for join_obj in self.joins:
                sql += f" {join_obj.__vw_render__(context)}"
        return sql


@dataclass
class Statement:
    """Represents a SQL statement."""

    source: Source
    columns: list[Expression]
    where_conditions: list[Expression] = field(default_factory=list)

    def where(self, *exprs: Expression) -> Statement:
        """
        Add WHERE conditions to the statement.

        Args:
            *exprs: Expression objects for WHERE clause. Multiple expressions are combined with AND.

        Returns:
            A new Statement with the WHERE conditions applied.

        Example:
            >>> from vw import col, param
            >>> Source("users").select(col("*")).where(col("age") >= param("min_age", 18))
            >>> Source("users").select(col("*")).where(col("status") == col("'active'"), col("age") >= col("18"))
        """
        return Statement(
            source=self.source,
            columns=self.columns,
            where_conditions=self.where_conditions + list(exprs),
        )

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """
        Render the SQL statement with parameter tracking.

        Args:
            config: Rendering configuration. Defaults to RenderConfig with COLON parameter style.

        Returns:
            RenderResult containing the SQL string and parameter dictionary.
        """
        if config is None:
            config = RenderConfig()
        context = RenderContext(config=config)
        sql = self.__vw_render__(context)
        return RenderResult(sql=sql, params=context.params)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the statement."""
        # Render columns
        rendered_columns = [col.__vw_render__(context) for col in self.columns]
        columns_str = ", ".join(rendered_columns)
        source_str = self.source.__vw_render__(context)

        sql = f"SELECT {columns_str} FROM {source_str}"

        # Add WHERE clause if conditions exist
        if self.where_conditions:
            conditions = [expr.__vw_render__(context) for expr in self.where_conditions]
            sql += f" WHERE {' AND '.join(conditions)}"

        return sql
