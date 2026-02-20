"""PostgreSQL-specific expression states."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from vw.core.states import Expr, Source

# --- Date/Time ------------------------------------------------------------- #


@dataclass(frozen=True, kw_only=True)
class Now(Expr):
    """Represents NOW() — PostgreSQL current timestamp function."""


@dataclass(frozen=True, kw_only=True)
class Interval(Expr):
    """Represents INTERVAL 'amount unit' — PostgreSQL interval literal."""

    amount: int | float
    unit: str


@dataclass(frozen=True, kw_only=True)
class DateTrunc(Expr):
    """Represents DATE_TRUNC('unit', expr) — PostgreSQL date truncation."""

    unit: str
    expr: Expr


# --- Row-Level Locking ----------------------------------------------------- #


@dataclass(frozen=True, kw_only=True)
class RowLock(Expr):
    """Represents a FOR UPDATE/SHARE locking clause (PostgreSQL row-level locking).
    """

    strength: Literal["UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"]
    '"UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"'
    of_tables: tuple[Source, ...] = ()
    'For Reference: alias or name. For other Sources: alias required.'
    wait_policy: Literal["NOWAIT", "SKIP LOCKED"] | None = None
    '"NOWAIT", "SKIP LOCKED"'
