"""SQL query builder with method chaining."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from vw.base import Expression, RowSet
from vw.column import Column
from vw.render import RenderConfig, RenderContext, RenderResult

if TYPE_CHECKING:
    pass


@dataclass(kw_only=True, frozen=True)
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


@dataclass(kw_only=True, frozen=True)
class CommonTableExpression(RowSet):
    """A Common Table Expression (CTE) for use in WITH clauses.

    CTEs are named subqueries that can be referenced like tables.
    They register themselves in the RenderContext during rendering,
    and the WITH clause is prepended by Statement.render().

    Example:
        >>> active_users = CommonTableExpression(
        ...     name="active_users",
        ...     query=Source(name="users").select(col("*")).where(col("active") == col("true"))
        ... )
        >>> result = active_users.select(col("*")).render()
        # WITH active_users AS (SELECT * FROM users WHERE ...) SELECT * FROM active_users
    """

    name: str
    query: Statement

    def col(self, column_name: str, /) -> Column:
        """Create a column qualified with the CTE name or alias.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the CTE name (or alias if set) as prefix.

        Example:
            >>> cte = CommonTableExpression(name="active_users", query=...)
            >>> cte.col("id")  # Returns Column("active_users.id")
            >>> cte.alias("au").col("id")  # Returns Column("au.id")
        """
        prefix = self._alias or self.name
        return Column(name=f"{prefix}.{column_name}")

    def __vw_render__(self, context: RenderContext) -> str:
        """Register CTE with pre-rendered body and return reference name."""
        # Render query first to discover dependencies, then register
        body_sql = self.query.__vw_render__(context.recurse())
        context.register_cte(self, body_sql)

        # Return just the reference name
        sql = self.name
        if self._alias:
            sql += f" AS {self._alias}"
        for join_obj in self._joins:
            sql += f" {join_obj.__vw_render__(context)}"
        return sql


def cte(name: str, query: Statement, /) -> CommonTableExpression:
    """Create a Common Table Expression (CTE).

    Args:
        name: The name for the CTE.
        query: The Statement that defines the CTE.

    Returns:
        A CommonTableExpression that can be used like a table.

    Example:
        >>> active_users = cte("active_users", Source(name="users").select(col("*")).where(...))
        >>> result = active_users.select(col("*")).render()
    """
    return CommonTableExpression(name=name, query=query)


@dataclass(kw_only=True, frozen=True)
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
        return Column(name=f"{prefix}.{column_name}")

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the source."""
        sql = self.name
        if self._alias:
            sql += f" AS {self._alias}"
        for join_obj in self._joins:
            sql += f" {join_obj.__vw_render__(context)}"
        return sql


@dataclass(kw_only=True, frozen=True)
class Statement(RowSet, Expression):
    """Represents a SQL statement.

    Statement extends both Expression and RowSet, allowing it to be used:
    - As a top-level query (via render())
    - As a subquery in WHERE clauses (as Expression)
    - As a subquery in FROM/JOIN clauses (as RowSet)
    """

    source: RowSet
    columns: list[Expression]
    where_conditions: list[Expression] = field(default_factory=list)
    group_by_columns: list[Expression] = field(default_factory=list)
    having_conditions: list[Expression] = field(default_factory=list)
    order_by_columns: list[Expression] = field(default_factory=list)

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
            group_by_columns=self.group_by_columns,
            having_conditions=self.having_conditions,
            order_by_columns=self.order_by_columns,
        )

    def group_by(self, *exprs: Expression) -> Statement:
        """
        Add GROUP BY columns to the statement.

        Args:
            *exprs: Expression objects for GROUP BY clause.

        Returns:
            A new Statement with the GROUP BY columns applied.

        Example:
            >>> from vw import col
            >>> Source(name="orders").select(col("customer_id"), col("SUM(total)")).group_by(col("customer_id"))
        """
        return Statement(
            source=self.source,
            columns=self.columns,
            where_conditions=self.where_conditions,
            group_by_columns=self.group_by_columns + list(exprs),
            having_conditions=self.having_conditions,
            order_by_columns=self.order_by_columns,
        )

    def having(self, *exprs: Expression) -> Statement:
        """
        Add HAVING conditions to the statement.

        Args:
            *exprs: Expression objects for HAVING clause. Multiple expressions are combined with AND.

        Returns:
            A new Statement with the HAVING conditions applied.

        Example:
            >>> from vw import col, param
            >>> Source(name="orders").select(col("customer_id"), col("COUNT(*)")).group_by(col("customer_id")).having(col("COUNT(*)") > param("min", 5))
        """
        return Statement(
            source=self.source,
            columns=self.columns,
            where_conditions=self.where_conditions,
            group_by_columns=self.group_by_columns,
            having_conditions=self.having_conditions + list(exprs),
            order_by_columns=self.order_by_columns,
        )

    def order_by(self, *exprs: Expression) -> Statement:
        """
        Add ORDER BY columns to the statement.

        Args:
            *exprs: Expression objects for ORDER BY clause. Use .asc() or .desc() for sort direction.

        Returns:
            A new Statement with the ORDER BY columns applied.

        Example:
            >>> from vw import col
            >>> Source(name="users").select(col("*")).order_by(col("name").asc(), col("created_at").desc())
        """
        return Statement(
            source=self.source,
            columns=self.columns,
            where_conditions=self.where_conditions,
            group_by_columns=self.group_by_columns,
            having_conditions=self.having_conditions,
            order_by_columns=self.order_by_columns + list(exprs),
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

        # Prepend WITH clause if CTEs were registered
        if context.ctes:
            cte_definitions = [f"{cte.name} AS {body_sql}" for cte, body_sql in context.ctes]
            sql = f"WITH {', '.join(cte_definitions)} {sql}"

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

        if self.group_by_columns:
            group_cols = [col.__vw_render__(context) for col in self.group_by_columns]
            sql += f" GROUP BY {', '.join(group_cols)}"

        if self.having_conditions:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self.having_conditions]
            sql += f" HAVING {' AND '.join(conditions)}"

        if self.order_by_columns:
            order_cols = [col.__vw_render__(context) for col in self.order_by_columns]
            sql += f" ORDER BY {', '.join(order_cols)}"

        # Parenthesize if nested
        if context.depth > 0:
            sql = f"({sql})"
            if self._alias:
                sql += f" AS {self._alias}"
        return sql
