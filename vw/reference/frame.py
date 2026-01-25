"""Window frame boundary definitions.

Usage:
    >>> from vw.reference import frame
    >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
    ...     frame.UNBOUNDED_PRECEDING, frame.CURRENT_ROW
    ... )
    >>> # With EXCLUDE clause
    >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
    ...     frame.UNBOUNDED_PRECEDING, frame.CURRENT_ROW
    ... ).exclude(frame.FrameExclude.CURRENT_ROW)
"""

from strenum import StrEnum


class FrameExclude(StrEnum):
    """Window frame EXCLUDE options."""

    NO_OTHERS = "NO OTHERS"
    "Default behavior - no rows are excluded from the frame"

    CURRENT_ROW = "CURRENT ROW"
    "Excludes the current row from the frame"

    GROUP = "GROUP"
    "Excludes the current row and all peers (rows equal in ORDER BY)"

    TIES = "TIES"
    "Excludes peers of the current row, but not the current row itself"


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
