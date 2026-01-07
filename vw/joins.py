"""SQL JOIN operations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, ClassVar

from vw.base import Expression, RowSet

if TYPE_CHECKING:
    from vw.render import RenderContext


@dataclass(kw_only=True, frozen=True)
class Join:
    """Base class for JOIN operations."""

    _keyword: ClassVar[str]

    right: RowSet
    on: Sequence[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the join."""
        join_sql = f"{self._keyword} {self.right.__vw_render__(context)}"
        if self.on:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        return join_sql


@dataclass(kw_only=True, frozen=True)
class InnerJoin(Join):
    """Represents an INNER JOIN operation."""

    _keyword: ClassVar[str] = "INNER JOIN"


@dataclass(kw_only=True, frozen=True)
class LeftJoin(Join):
    """Represents a LEFT JOIN operation."""

    _keyword: ClassVar[str] = "LEFT JOIN"


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

    def left(self, right: RowSet, *, on: Sequence[Expression] = ()) -> RowSet:
        """
        Perform a LEFT JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [LeftJoin(right=right, on=on)],
        )
