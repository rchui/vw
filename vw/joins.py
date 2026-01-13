"""SQL JOIN operations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from strenum import StrEnum

from vw.base import Expression, RowSet
from vw.exceptions import JoinConditionError

if TYPE_CHECKING:
    from vw.render import RenderContext


class JoinType(StrEnum):
    """SQL JOIN types."""

    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL_OUTER = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"
    SEMI = "SEMI JOIN"
    ANTI = "ANTI JOIN"


@dataclass(kw_only=True, frozen=True)
class Join:
    """Base class for JOIN operations."""

    jtype: JoinType
    right: RowSet
    on: Sequence[Expression] = field(default_factory=list)
    using: Sequence[Expression] = field(default_factory=list)
    lateral: bool = False

    def __post_init__(self) -> None:
        """Validate join conditions."""
        if self.on and self.using:
            raise JoinConditionError("Cannot specify both ON and USING in a join")

    def __vw_render__(self, context: RenderContext) -> str:
        """Return the SQL representation of the join."""
        # Build the join type string
        join_type_str = str(self.jtype)

        # Build the right side (with LATERAL prefix if needed)
        # Use recurse() so subqueries get parenthesized
        right_sql = self.right.__vw_render__(context.recurse())
        if self.lateral:
            right_sql = f"LATERAL {right_sql}"

        join_sql = f"{join_type_str} {right_sql}"

        # Add ON or USING clause
        if self.on:
            conditions = [f"({expr.__vw_render__(context)})" for expr in self.on]
            join_sql += f" ON {' AND '.join(conditions)}"
        elif self.using:
            col_names = [expr.__vw_render__(context) for expr in self.using]
            join_sql += f" USING ({', '.join(col_names)})"

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


@dataclass(kw_only=True, frozen=True)
class SemiJoin(Join):
    """Represents a SEMI JOIN operation."""

    jtype: JoinType = JoinType.SEMI


@dataclass(kw_only=True, frozen=True)
class AntiJoin(Join):
    """Represents an ANTI JOIN operation."""

    jtype: JoinType = JoinType.ANTI


class JoinAccessor:
    """Accessor for join operations on a RowSet."""

    def __init__(self, row_set: RowSet):
        self._row_set = row_set

    def inner(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform an INNER JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            >>> users.join.inner(orders, using=[col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [InnerJoin(right=right, on=on, using=using, lateral=lateral)],
        )

    def left(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform a LEFT JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
            >>> users.join.left(orders, using=[col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [LeftJoin(right=right, on=on, using=using, lateral=lateral)],
        )

    def right(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform a RIGHT JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.right(orders, on=[users.col("id") == orders.col("user_id")])
            >>> users.join.right(orders, using=[col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [RightJoin(right=right, on=on, using=using, lateral=lateral)],
        )

    def full_outer(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform a FULL OUTER JOIN with another row set.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")])
            >>> users.join.full_outer(orders, using=[col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [FullOuterJoin(right=right, on=on, using=using, lateral=lateral)],
        )

    def cross(self, right: RowSet, *, lateral: bool = False) -> RowSet:
        """
        Perform a CROSS JOIN with another row set.

        A CROSS JOIN returns the Cartesian product of both tables.
        No ON or USING condition is used.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> colors = Source(name="colors")
            >>> sizes = Source(name="sizes")
            >>> colors.join.cross(sizes)
            >>> users.join.cross(subquery, lateral=True)
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [CrossJoin(right=right, lateral=lateral)],
        )

    def semi(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform a SEMI JOIN with another row set.

        A SEMI JOIN returns rows from the left table that have matching rows
        in the right table, without duplicating rows.

        Note: SEMI JOIN is not supported by all databases. Spark SQL and some
        others support it natively. For other databases, use EXISTS subqueries.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.semi(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [SemiJoin(right=right, on=on, using=using, lateral=lateral)],
        )

    def anti(
        self,
        right: RowSet,
        *,
        on: Sequence[Expression] = (),
        using: Sequence[Expression] = (),
        lateral: bool = False,
    ) -> RowSet:
        """
        Perform an ANTI JOIN with another row set.

        An ANTI JOIN returns rows from the left table that have no matching rows
        in the right table.

        Note: ANTI JOIN is not supported by all databases. Spark SQL and some
        others support it natively. For other databases, use NOT EXISTS subqueries.

        Args:
            right: The row set to join with (table, subquery, or CTE).
            on: Sequence of join condition expressions. Multiple conditions are combined with AND.
            using: Sequence of column expressions to join on (alternative to ON). Cannot be used with `on`.
            lateral: If True, the right side can reference columns from the left side.

        Returns:
            A new RowSet with the join applied.

        Example:
            >>> users = Source(name="users")
            >>> orders = Source(name="orders")
            >>> users.join.anti(orders, on=[users.col("id") == orders.col("user_id")])
        """
        return replace(
            self._row_set,
            _joins=self._row_set._joins + [AntiJoin(right=right, on=on, using=using, lateral=lateral)],
        )
