"""Base classes for SQL expression building."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vw.build import InnerJoin, JoinAccessor, Statement
    from vw.column import Column
    from vw.operators import Alias, And, Not, Or
    from vw.render import RenderContext

from typing_extensions import Self


@dataclass(kw_only=True, frozen=True)
class RowSet:
    """Base class for things that produce rows (tables, subqueries, CTEs).

    Used in FROM and JOIN clauses.
    """

    _alias: str | None = None
    _joins: list[InnerJoin] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the row set."""
        raise NotImplementedError

    def alias(self, name: str, /) -> Self:
        """Create an aliased copy of this row set.

        Args:
            name: The alias name.

        Returns:
            A copy of this row set with the alias set.

        Example:
            >>> Source(name="users").alias("u")
            >>> subquery.alias("sq")
        """
        return replace(self, _alias=name)

    def col(self, column_name: str, /) -> Column:
        """Create a column reference qualified with this row set's alias.

        Args:
            column_name: Column name to qualify.

        Returns:
            A Column with the alias as prefix.

        Example:
            >>> Source(name="users").alias("u").col("id")  # Returns Column("u.id")
        """
        from vw.column import Column

        if self._alias:
            return Column(name=f"{self._alias}.{column_name}")
        return Column(name=column_name)

    @property
    def join(self) -> JoinAccessor:
        """Access join operations."""
        from vw.build import JoinAccessor

        return JoinAccessor(self)

    def select(self, *columns: Expression) -> Statement:
        """
        Select columns from this row set.

        Args:
            *columns: Expression objects to select.

        Returns:
            A Statement object for method chaining.

        Example:
            >>> from vw import col
            >>> Source(name="users").select(col("*"))
            >>> subquery.alias("sq").select(col("*"))
        """
        from vw.build import Statement

        return Statement(source=self, columns=list(columns))


class Expression:
    """Protocol for SQL expressions."""

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the expression."""
        raise NotImplementedError

    def __and__(self, other: Expression) -> And:
        """Create a logical AND expression with another expression

        Args:
            other: The other expression to combine with.

        Returns:
            An And expression representing the logical AND of this and the other expression.

        Example:
            >>> expr1 = col("age") > param("min_age", 18)
            >>> expr2 = col("status") == param("active_status", "active")
            >>> combined_expr = expr1 & expr2
        """
        from vw.operators import And

        return And(left=self, right=other)

    def __or__(self, other: Expression) -> Or:
        """Create a logical OR expression with another expression

        Args:
            other: The other expression to combine with.

        Returns:
            An Or expression representing the logical OR of this and the other expression.

        Example:
            >>> expr1 = col("age") < param("max_age", 65)
            >>> expr2 = col("status") == param("inactive_status", "inactive")
            >>> combined_expr = expr1 | expr2
        """
        from vw.operators import Or

        return Or(left=self, right=other)

    def __invert__(self) -> Not:
        """Create a logical NOT expression

        Returns:
            A Not expression representing the logical NOT of this expression.

        Example:
            >>> expr = col("active") == col("true")
            >>> negated = ~expr
        """
        from vw.operators import Not

        return Not(operand=self)

    def alias(self, name: str, /) -> Alias:
        """Create an aliased expression.

        Args:
            name: The alias name.

        Returns:
            An Alias expression.

        Example:
            >>> col("price").alias("unit_price")
        """
        from vw.operators import Alias

        return Alias(expr=self, name=name)
