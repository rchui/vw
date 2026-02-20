"""PostgreSQL-specific modifier factories for use with .modifiers()."""

from typing import Literal

from vw.core.base import Factories
from vw.core.states import Source
from vw.postgres.base import Expression, RowSet
from vw.postgres.states import RowLock


def row_lock(
    strength: Literal["UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"],
    *,
    of: tuple[Source, ...] = (),
    wait_policy: Literal["NOWAIT", "SKIP LOCKED"] | None = None,
) -> Expression:
    """Create a FOR UPDATE/SHARE row-level lock modifier.

    Args:
        strength: Lock strength — "UPDATE", "NO KEY UPDATE", "SHARE", or "KEY SHARE".
        of: Sources to lock (uses alias if set, otherwise name for References).
        wait_policy: "NOWAIT" or "SKIP LOCKED", or None to wait (default).

    Returns:
        An Expression wrapping a RowLock state, for use with .modifiers().

    Examples:
        >>> ref("jobs").select(col("*")).modifiers(row_lock("UPDATE"))
        >>> ref("jobs").select(col("*")).modifiers(row_lock("UPDATE", wait_policy="SKIP LOCKED"))
    """
    state = RowLock(strength=strength, of_tables=of, wait_policy=wait_policy)
    return Expression(state=state, factories=Factories(expr=Expression, rowset=RowSet))
