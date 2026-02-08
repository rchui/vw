"""Date/time function accessor for Expression (.dt property)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT

if TYPE_CHECKING:
    from vw.core.base import Expression


class DateTimeAccessor(Generic[ExprT, RowSetT]):
    """Date/time function accessor for Expression (.dt property).

    Provides ANSI SQL date/time functions as methods on a date/time-typed expression.
    Access via: col("created_at").dt.extract("year")
    """

    def __init__(self, expr: Expression[ExprT, RowSetT]) -> None:
        self.expr = expr

    def extract(self, field: str) -> ExprT:
        """EXTRACT(field FROM expr) — extract a date/time component.

        Args:
            field: The component to extract (e.g. "year", "month", "day", "hour",
                   "minute", "second", "epoch", etc.). Passed directly to SQL.
        """
        from vw.core.states import Extract

        state = Extract(field=field, expr=self.expr.state)
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def quarter(self) -> ExprT:
        """EXTRACT(QUARTER FROM expr) — extract the quarter (1-4)."""
        return self.extract("quarter")

    def week(self) -> ExprT:
        """EXTRACT(WEEK FROM expr) — extract the ISO week number."""
        return self.extract("week")

    def weekday(self) -> ExprT:
        """EXTRACT(DOW FROM expr) — extract the day of week (0=Sunday)."""
        return self.extract("dow")
