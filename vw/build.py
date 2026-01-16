"""SQL query builder with method chaining."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from typing_extensions import TypeAlias

from vw.base import Expression, RowSet
from vw.column import Column
from vw.exceptions import UnsupportedDialectError
from vw.render import Dialect, RenderConfig, RenderContext, RenderResult

if TYPE_CHECKING:
    from vw.ddl import TableAccessor, ViewAccessor


@dataclass(kw_only=True, frozen=True)
class Limit:
    """Represents LIMIT and optional OFFSET for pagination."""

    count: int
    offset: int | None = None


@dataclass(kw_only=True, frozen=True)
class Distinct:
    """Represents DISTINCT or DISTINCT ON clause.

    When `on` is None, renders as DISTINCT.
    When `on` is a list of expressions, renders as DISTINCT ON (PostgreSQL only).
    """

    on: list[Expression] | None = None


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

    @property
    def table(self) -> TableAccessor:
        """Access table DDL operations."""
        from vw.ddl import TableAccessor

        return TableAccessor(self)

    @property
    def view(self) -> ViewAccessor:
        """Access view DDL operations."""
        from vw.ddl import ViewAccessor

        return ViewAccessor(self)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the source."""
        sql = self.name
        if self._alias:
            sql += f" AS {self._alias}"
        for join_obj in self._joins:
            sql += f" {join_obj.__vw_render__(context)}"
        return sql


# Type alias for statements that can be combined with set operators
Combinable: TypeAlias = "Statement | SetOperation"


@dataclass(kw_only=True, frozen=True)
class Statement(RowSet, Expression):
    """Represents a SQL statement.

    Statement extends both Expression and RowSet, allowing it to be used:
    - As a top-level query (via render())
    - As a subquery in WHERE clauses (as Expression)
    - As a subquery in FROM/JOIN clauses (as RowSet)
    - Combined with other statements via set operators (UNION, INTERSECT, EXCEPT)
    """

    source: RowSet
    columns: list[Expression]
    where_conditions: list[Expression] = field(default_factory=list)
    group_by_columns: list[Expression] = field(default_factory=list)
    having_conditions: list[Expression] = field(default_factory=list)
    order_by_columns: list[Expression] = field(default_factory=list)
    _limit: Limit | None = None
    _distinct: Distinct | None = None

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
        return replace(self, where_conditions=self.where_conditions + list(exprs))

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
        return replace(self, group_by_columns=self.group_by_columns + list(exprs))

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
        return replace(self, having_conditions=self.having_conditions + list(exprs))

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
        return replace(self, order_by_columns=self.order_by_columns + list(exprs))

    def limit(self, n: int, /, *, offset: int | None = None) -> Statement:
        """
        Add LIMIT and optional OFFSET to the statement.

        Args:
            n: Maximum number of rows to return.
            offset: Number of rows to skip before returning results.

        Returns:
            A new Statement with the LIMIT applied.

        Example:
            >>> from vw import col
            >>> Source(name="users").select(col("*")).limit(10)
            >>> Source(name="users").select(col("*")).order_by(col("id").asc()).limit(10, offset=20)
        """
        return replace(self, _limit=Limit(count=n, offset=offset))

    def distinct(self, *, on: list[Expression] | None = None) -> Statement:
        """Return only distinct rows.

        Args:
            on: Optional columns for DISTINCT ON (PostgreSQL only).

        Returns:
            A new Statement with DISTINCT applied.

        Example:
            >>> Source(name="users").select(col("name")).distinct()
            >>> Source(name="users").select(col("dept"), col("name")).distinct(on=[col("dept")])
        """
        return replace(self, _distinct=Distinct(on=on))

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

        sql = "SELECT"
        if self._distinct is not None:
            if self._distinct.on is not None:
                on_cols = ", ".join(col.__vw_render__(context) for col in self._distinct.on)
                sql += f" DISTINCT ON ({on_cols})"
            else:
                sql += " DISTINCT"
        sql += f" {columns_str} FROM {source_str}"

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

        if self._limit is not None:
            if context.config.dialect == Dialect.POSTGRES:
                sql += f" LIMIT {self._limit.count}"
                if self._limit.offset is not None:
                    sql += f" OFFSET {self._limit.offset}"
            elif context.config.dialect == Dialect.SQLSERVER:
                offset = self._limit.offset or 0
                sql += f" OFFSET {offset} ROWS FETCH NEXT {self._limit.count} ROWS ONLY"
            else:
                raise UnsupportedDialectError(f"Unsupported dialect for LIMIT: {context.config.dialect}")

        # Parenthesize if nested
        if context.depth > 0:
            sql = f"({sql})"
            if self._alias:
                sql += f" AS {self._alias}"
        return sql

    # Intentionally override Expression's logical operators with set operators.
    # On Expression: | means OR, & means AND (logical operators for WHERE conditions)
    # On Statement/SetOperation: | means UNION, & means INTERSECT (SQL set operators)
    # This mirrors Python's semantics where | and & behave differently for sets vs bools.

    def __or__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using UNION (deduplicated).

        Args:
            other: The query to union with.

        Returns:
            A SetOperation representing the UNION.

        Example:
            >>> query1 | query2  # SELECT ... UNION SELECT ...
        """
        return SetOperation(left=self, operator="UNION", right=other)

    def __add__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using UNION ALL (keeps duplicates).

        Args:
            other: The query to union with.

        Returns:
            A SetOperation representing the UNION ALL.

        Example:
            >>> query1 + query2  # SELECT ... UNION ALL SELECT ...
        """
        return SetOperation(left=self, operator="UNION ALL", right=other)

    def __and__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using INTERSECT.

        Args:
            other: The query to intersect with.

        Returns:
            A SetOperation representing the INTERSECT.

        Example:
            >>> query1 & query2  # SELECT ... INTERSECT SELECT ...
        """
        return SetOperation(left=self, operator="INTERSECT", right=other)

    def __sub__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using EXCEPT.

        Args:
            other: The query to except.

        Returns:
            A SetOperation representing the EXCEPT.

        Example:
            >>> query1 - query2  # SELECT ... EXCEPT SELECT ...
        """
        return SetOperation(left=self, operator="EXCEPT", right=other)


@dataclass(kw_only=True, frozen=True)
class SetOperation(RowSet, Expression):
    """Represents combined queries with UNION/INTERSECT/EXCEPT.

    SetOperation extends both Expression and RowSet, allowing it to be used:
    - As a top-level query (via render())
    - As a subquery in WHERE clauses (as Expression)
    - As a subquery in FROM/JOIN clauses (as RowSet)
    - Combined further with other statements via set operators

    Example:
        >>> query1 = Source(name="users").select(col("id"))
        >>> query2 = Source(name="admins").select(col("id"))
        >>> combined = query1 | query2  # UNION
        >>> combined = query1 + query2  # UNION ALL
        >>> combined = query1 & query2  # INTERSECT
        >>> combined = query1 - query2  # EXCEPT
    """

    left: Combinable
    operator: str  # "UNION", "UNION ALL", "INTERSECT", "EXCEPT"
    right: Combinable

    def render(self, config: RenderConfig | None = None) -> RenderResult:
        """
        Render the compound SQL statement with parameter tracking.

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
        """Return the SQL representation of the set operation."""
        nested = context.recurse()
        left_sql = self.left.__vw_render__(nested)
        right_sql = self.right.__vw_render__(nested)

        sql = f"{left_sql} {self.operator} {right_sql}"

        # Parenthesize if nested
        if context.depth > 0:
            sql = f"({sql})"
            if self._alias:
                sql += f" AS {self._alias}"
        return sql

    # See comment in Statement for rationale on overriding Expression's operators.

    def __or__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using UNION."""
        return SetOperation(left=self, operator="UNION", right=other)

    def __add__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using UNION ALL."""
        return SetOperation(left=self, operator="UNION ALL", right=other)

    def __and__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using INTERSECT."""
        return SetOperation(left=self, operator="INTERSECT", right=other)

    def __sub__(self, other: Combinable) -> SetOperation:  # type: ignore[override]
        """Combine with another query using EXCEPT."""
        return SetOperation(left=self, operator="EXCEPT", right=other)
