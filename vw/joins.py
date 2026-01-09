"""SQL JOIN operations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from strenum import StrEnum

from vw.base import Expression, RowSet

if TYPE_CHECKING:
    from vw.render import RenderContext


class JoinType(StrEnum):
    """SQL JOIN types."""

    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL_OUTER = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"


@dataclass(kw_only=True, frozen=True)
class Join:
    """Base class for JOIN operations."""

    jtype: JoinType
    right: RowSet
    on: Sequence[Expression] = field(default_factory=list)

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the join."""
        join_sql = f"{self.jtype} {self.right.__vw_render__(context)}"
        if self.on:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        return join_sql


@dataclass(kw_only=True, frozen=True)
class InnerJoin(Join):
    """Represents an INNER JOIN operation."""

    jtype: JoinType = JoinType.INNER


@dataclass(kw_only=True, frozen=True)
class LeftJoin(Join):
    """Represents a LEFT JOIN operation."""

    jtype: JoinType = JoinType.LEFT


@dataclass(kw_only=True, frozen=True)
class RightJoin(Join):
    """Represents a RIGHT JOIN operation."""

    jtype: JoinType = JoinType.RIGHT


@dataclass(kw_only=True, frozen=True)
class FullOuterJoin(Join):
    """Represents a FULL OUTER JOIN operation."""

    jtype: JoinType = JoinType.FULL_OUTER


@dataclass(kw_only=True, frozen=True)
class CrossJoin(Join):
    """Represents a CROSS JOIN operation."""

    jtype: JoinType = JoinType.CROSS


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

    def right(self, right: RowSet, *, on: Sequence[Expression] = ()) -> RowSet:
        """
        Perform a RIGHT JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.right(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [RightJoin(right=right, on=on)],
        )

    def full_outer(self, right: RowSet, *, on: Sequence[Expression] = ()) -> RowSet:
        """
        Perform a FULL OUTER JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [FullOuterJoin(right=right, on=on)],
        )

    def cross(self, right: RowSet) -> RowSet:
        """
        Perform a CROSS JOIN with another row set.

        A CROSS JOIN returns the Cartesian product of both tables.
        No ON condition is used.

        Args:
            right: The row set to join with (table, subquery, or CTE).

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> colors = Source(name="colors")
            >>> sizes = Source(name="sizes")
            >>> colors.join.cross(sizes)
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [CrossJoin(right=right)],
        )
