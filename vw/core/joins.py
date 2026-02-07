from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT, SetOpT

if TYPE_CHECKING:
    from vw.core.base import RowSet
    from vw.core.states import JoinType


def add_join(
    rowset: RowSet[ExprT, RowSetT, SetOpT],
    jtype: JoinType,
    right: RowSet[ExprT, RowSetT, SetOpT],
    on: list[ExprT],
    using: list[ExprT],
) -> RowSet[ExprT, RowSetT, SetOpT]:
    """Add a join to a rowset.

    Args:
        rowset: The rowset to add the join to.
        jtype: The type of join (INNER, LEFT, RIGHT, FULL, CROSS).
        right: The table/subquery to join.
        on: Join conditions (combined with AND).
        using: Column names for USING clause.

    Returns:
        A new RowSet with the join added.
    """
    from vw.core.states import Join, Reference, Statement

    # Ensure we have a Statement
    if isinstance(rowset.state, Reference):
        stmt = Statement(source=rowset.state)
    else:
        stmt = rowset.state

    # Create join
    join = Join(
        jtype=jtype,
        right=right.state,
        on=tuple(column for column in on),
        using=tuple(column for column in using),
    )

    # Accumulate join
    new_state = replace(stmt, joins=stmt.joins + (join,))
    return rowset.factories.rowset(state=new_state, factories=rowset.factories)


@dataclass(eq=False, frozen=True, kw_only=True)
class JoinAccessor(Generic[ExprT, RowSetT, SetOpT]):
    """Accessor for join operations on a RowSet."""

    _rowset: RowSet[ExprT, RowSetT, SetOpT]

    def inner(
        self, right: RowSetT, *, on: list[ExprT] | None = None, using: list[ExprT] | None = None
    ) -> RowSet[ExprT, RowSetT, SetOpT]:
        """Create an INNER JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.INNER, right, on or [], using or [])

    def left(
        self, right: RowSetT, *, on: list[ExprT] | None = None, using: list[ExprT] | None = None
    ) -> RowSet[ExprT, RowSetT, SetOpT]:
        """Create a LEFT JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.LEFT, right, on or [], using or [])

    def right(
        self, right: RowSetT, *, on: list[ExprT] | None = None, using: list[ExprT] | None = None
    ) -> RowSet[ExprT, RowSetT, SetOpT]:
        """Create a RIGHT JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.RIGHT, right, on or [], using or [])

    def full_outer(
        self, right: RowSetT, *, on: list[ExprT] | None = None, using: list[ExprT] | None = None
    ) -> RowSet[ExprT, RowSetT, SetOpT]:
        """Create a FULL OUTER JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.FULL, right, on or [], using or [])

    def cross(self, right: RowSetT) -> RowSet[ExprT, RowSetT, SetOpT]:
        """Create a CROSS JOIN.

        Args:
            right: The table/subquery to join.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.CROSS, right, [], [])
