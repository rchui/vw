from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT

if TYPE_CHECKING:
    from vw.core.base import RowSet


def add_join(
    *,
    rowset: RowSet[ExprT, RowSetT],
    jtype: str,
    right: RowSet[ExprT, RowSetT],
    on: list[ExprT],
    using: list[ExprT],
    lateral: bool = False,
) -> RowSet[ExprT, RowSetT]:
    """Add a join to a rowset.

    Args:
        rowset: The rowset to add the join to.
        jtype: The type of join (INNER, LEFT, RIGHT, FULL, CROSS).
        right: The table/subquery to join.
        on: Join conditions (combined with AND).
        using: Column names for USING clause.
        lateral: If True, create a LATERAL join (allows right side to reference left side).

    Returns:
        A new RowSet with the join added.
    """
    from vw.core.states import Join, RawSource, Reference, SetOperation, Statement, Values

    # Ensure we have a Statement
    if isinstance(rowset.state, (Reference, SetOperation, Values, RawSource)):
        stmt = Statement(source=rowset.state)
    else:
        stmt = rowset.state

    # Create join
    join = Join(
        jtype=jtype,
        right=right.state,
        on=tuple(c.state for c in on),
        using=tuple(c.state for c in using),
        lateral=lateral,
    )

    # Accumulate join
    new_state = replace(stmt, joins=stmt.joins + (join,))
    return rowset.factories.rowset(state=new_state, factories=rowset.factories)


@dataclass(eq=False, frozen=True, kw_only=True)
class JoinAccessor(Generic[ExprT, RowSetT]):
    """Accessor for join operations on a RowSet."""

    _rowset: RowSet[ExprT, RowSetT]

    def inner(
        self,
        right: RowSetT,
        *,
        on: list[ExprT] | None = None,
        using: list[ExprT] | None = None,
        lateral: bool = False,
    ) -> RowSet[ExprT, RowSetT]:
        """Create an INNER JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.
            lateral: If True, create a LATERAL join (allows right side to reference left side).

        Returns:
            A new RowSet with the join added.
        """
        return add_join(
            rowset=self._rowset,
            jtype="INNER",
            right=right,
            on=on or [],
            using=using or [],
            lateral=lateral,
        )

    def left(
        self,
        right: RowSetT,
        *,
        on: list[ExprT] | None = None,
        using: list[ExprT] | None = None,
        lateral: bool = False,
    ) -> RowSet[ExprT, RowSetT]:
        """Create a LEFT JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.
            lateral: If True, create a LATERAL join (allows right side to reference left side).

        Returns:
            A new RowSet with the join added.
        """
        return add_join(
            rowset=self._rowset,
            jtype="LEFT",
            right=right,
            on=on or [],
            using=using or [],
            lateral=lateral,
        )

    def right(
        self,
        right: RowSetT,
        *,
        on: list[ExprT] | None = None,
        using: list[ExprT] | None = None,
        lateral: bool = False,
    ) -> RowSet[ExprT, RowSetT]:
        """Create a RIGHT JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.
            lateral: If True, create a LATERAL join (allows right side to reference left side).

        Returns:
            A new RowSet with the join added.
        """
        return add_join(
            rowset=self._rowset,
            jtype="RIGHT",
            right=right,
            on=on or [],
            using=using or [],
            lateral=lateral,
        )

    def full_outer(
        self,
        right: RowSetT,
        *,
        on: list[ExprT] | None = None,
        using: list[ExprT] | None = None,
        lateral: bool = False,
    ) -> RowSet[ExprT, RowSetT]:
        """Create a FULL OUTER JOIN.

        Args:
            right: The table/subquery to join.
            on: Join conditions (combined with AND).
            using: Column names for USING clause.
            lateral: If True, create a LATERAL join (allows right side to reference left side).

        Returns:
            A new RowSet with the join added.
        """
        return add_join(
            rowset=self._rowset,
            jtype="FULL",
            right=right,
            on=on or [],
            using=using or [],
            lateral=lateral,
        )

    def cross(self, right: RowSetT, *, lateral: bool = False) -> RowSet[ExprT, RowSetT]:
        """Create a CROSS JOIN.

        Args:
            right: The table/subquery to join.
            lateral: If True, create a LATERAL join (allows right side to reference left side).

        Returns:
            A new RowSet with the join added.
        """
        return add_join(
            rowset=self._rowset,
            jtype="CROSS",
            right=right,
            on=[],
            using=[],
            lateral=lateral,
        )
