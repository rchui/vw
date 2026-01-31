"""Window frame boundary definitions.

Usage:
    >>> from vw.postgres import F, col
    >>> from vw.postgres import UNBOUNDED_PRECEDING, CURRENT_ROW, preceding
    >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
    ...     UNBOUNDED_PRECEDING, CURRENT_ROW
    ... )
    >>> # With n PRECEDING
    >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
    ...     preceding(3), CURRENT_ROW
    ... )
"""

from __future__ import annotations

from vw.core.states import (
    CurrentRow,
    Following,
    Preceding,
    UnboundedFollowing,
    UnboundedPreceding,
)

# Singleton instances for common frame boundaries
UNBOUNDED_PRECEDING = UnboundedPreceding()
UNBOUNDED_FOLLOWING = UnboundedFollowing()
CURRENT_ROW = CurrentRow()


def preceding(n: int) -> Preceding:
    """Create an n PRECEDING frame boundary.

    Args:
        n: Number of rows preceding the current row.

    Returns:
        A Preceding state.

    Example:
        >>> preceding(3)  # 3 PRECEDING
    """
    return Preceding(count=n)


def following(n: int) -> Following:
    """Create an n FOLLOWING frame boundary.

    Args:
        n: Number of rows following the current row.

    Returns:
        A Following state.

    Example:
        >>> following(3)  # 3 FOLLOWING
    """
    return Following(count=n)
