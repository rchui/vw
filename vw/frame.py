"""Window frame boundary definitions.

Usage:
    >>> from vw import frame
    >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
    ...     frame.UNBOUNDED_PRECEDING, frame.CURRENT_ROW
    ... )
"""


class FrameBoundary:
    """Base class for window frame boundaries."""

    pass


class _UnboundedPreceding(FrameBoundary):
    """UNBOUNDED PRECEDING boundary."""

    def __str__(self) -> str:
        return "UNBOUNDED PRECEDING"


class _UnboundedFollowing(FrameBoundary):
    """UNBOUNDED FOLLOWING boundary."""

    def __str__(self) -> str:
        return "UNBOUNDED FOLLOWING"


class _CurrentRow(FrameBoundary):
    """CURRENT ROW boundary."""

    def __str__(self) -> str:
        return "CURRENT ROW"


class preceding(FrameBoundary):
    """n PRECEDING boundary."""

    def __init__(self, n: int) -> None:
        self.n = n

    def __str__(self) -> str:
        return f"{self.n} PRECEDING"


class following(FrameBoundary):
    """n FOLLOWING boundary."""

    def __init__(self, n: int) -> None:
        self.n = n

    def __str__(self) -> str:
        return f"{self.n} FOLLOWING"


# Singleton instances for common boundaries
UNBOUNDED_PRECEDING = _UnboundedPreceding()
UNBOUNDED_FOLLOWING = _UnboundedFollowing()
CURRENT_ROW = _CurrentRow()
