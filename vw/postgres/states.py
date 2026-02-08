"""PostgreSQL-specific expression states."""

from __future__ import annotations

from dataclasses import dataclass

from vw.core.states import Expr

# --- Date/Time ------------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Now(Expr):
    """Represents NOW() — PostgreSQL current timestamp function."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Interval(Expr):
    """Represents INTERVAL 'amount unit' — PostgreSQL interval literal."""

    amount: int | float
    unit: str


@dataclass(eq=False, frozen=True, kw_only=True)
class DateTrunc(Expr):
    """Represents DATE_TRUNC('unit', expr) — PostgreSQL date truncation."""

    unit: str
    expr: Expr
