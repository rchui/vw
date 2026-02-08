from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT

if TYPE_CHECKING:
    from vw.core.base import RowSet
    from vw.core.states import JoinType


def add_join(
    rowset: RowSet[ExprT, RowSetT],
    jtype: JoinType,
    right: RowSet[ExprT, RowSetT],
    on: list[ExprT],
    using: list[ExprT],
) -> RowSet[ExprT, RowSetT]:
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
    from vw.core.states import Join, Reference, SetOperation, Statement, Values

    # Ensure we have a Statement
    if isinstance(rowset.state, (Reference, SetOperation, Values)):
        stmt = Statement(source=rowset.state)
    else:
        stmt = rowset.state

    # Create join
    join = Join(
        jtype=jtype,
        right=right.state,
        on=tuple(c.state for c in on),
        using=tuple(c.state for c in using),
    )

    # Accumulate join
    new_state = replace(stmt, joins=stmt.joins + (join,))
    return rowset.factories.rowset(state=new_state, factories=rowset.factories)


@dataclass(eq=False, frozen=True, kw_only=True)
class JoinAccessor(Generic[ExprT, RowSetT]):
    """Accessor for join operations on a RowSet."""

    _rowset: RowSet[ExprT, RowSetT]

    def inner(
        self, right: RowSetT, *, on: list[ExprT] | None = None, using: list[ExprT] | None = None
    ) -> RowSet[ExprT, RowSetT]:
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
    ) -> RowSet[ExprT, RowSetT]:
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
    ) -> RowSet[ExprT, RowSetT]:
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
    ) -> RowSet[ExprT, RowSetT]:
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

    def cross(self, right: RowSetT) -> RowSet[ExprT, RowSetT]:
        """Create a CROSS JOIN.

        Args:
            right: The table/subquery to join.

        Returns:
            A new RowSet with the join added.
        """
        from vw.core.states import JoinType

        return add_join(self._rowset, JoinType.CROSS, right, [], [])
