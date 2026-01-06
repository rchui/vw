"""SQL query builder with method chaining."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from vw.expr import Column, Expression, RowSet
from vw.render import RenderConfig, RenderContext, RenderResult


@dataclass
class InnerJoin:
    """Represents an INNER JOIN operation."""

    right: RowSet
    on: Sequence[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the inner join."""

        join_sql = f"INNER JOIN {self.right.__vw_render__(context)}"
        if self.on:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        return join_sql


class JoinAccessor:
    """Accessor for join operations on a RowSet."""

    def __init__(self, row_set: RowSet):
        self._row_set = row_set

    def inner(self, right: RowSet, *, on: Sequence[Expression] = ()) -> RowSet:
        """
        Perform an INNER JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [InnerJoin(right=right, on=on)],
        )


@dataclass
class Source(RowSet):
    """Represents a SQL data source (table, view, etc.)."""

    name: str

    def col(self, column_name: str, /) -> Column:
        """
        Create a column reference qualified with this source's alias or name.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the alias (if set) or source name as a prefix.

        Example:
            >>> Source(name="users").col("id")  # Returns Column("users.id")
            >>> Source(name="users").alias("u").col("id")  # Returns Column("u.id")
        """
        prefix = self._alias or self.name
        return Column(f"{prefix}.{column_name}")

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the source."""
        sql = self.name
        if self._alias:
            sql += f" AS {self._alias}"
        for join_obj in self._joins:
            sql += f" {join_obj.__vw_render__(context)}"
        return sql


@dataclass
class Statement(Expression, RowSet):
    """Represents a SQL statement.

    Statement extends both Expression and RowSet, allowing it to be used:
    - As a top-level query (via render())
    - As a subquery in WHERE clauses (as Expression)
    - As a subquery in FROM/JOIN clauses (as RowSet)
    """

    source: RowSet
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
            >>> Source(name="users").select(col("*")).where(col("age") >= param("min_age", 18))
            >>> Source(name="users").select(col("*")).where(col("status") == col("'active'"), col("age") >= col("18"))
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
        rendered_columns = [col.__vw_render__(context) for col in self.columns]
        columns_str = ", ".join(rendered_columns)
        source_str = self.source.__vw_render__(context.recurse())

        sql = f"SELECT {columns_str} FROM {source_str}"

        if self.where_conditions:
            conditions: list[str] = [f"({expr.__vw_render__(context)})" for expr in self.where_conditions]
            sql += f" WHERE {' AND '.join(conditions)}"

        # Parenthesize if nested
        if context.depth > 0:
            sql = f"({sql})"
            if self._alias:
                sql += f" AS {self._alias}"
        return sql
