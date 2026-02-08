"""PostgreSQL date/time function accessor (.dt property extension)."""

from __future__ import annotations

from vw.core.base import ExprT, RowSetT
from vw.core.datetime import DateTimeAccessor


class PostgresDateTimeAccessor(DateTimeAccessor[ExprT, RowSetT]):
    """PostgreSQL date/time accessor.

    Extends the ANSI SQL DateTimeAccessor with PostgreSQL-specific functions.
    Access via: col("created_at").dt.extract("year")
    """

    def date_trunc(self, unit: str) -> ExprT:
        """DATE_TRUNC('unit', expr) — truncate timestamp to specified precision.

        Args:
            unit: The precision to truncate to (e.g. "year", "month", "day",
                  "hour", "minute", "second", "week", "quarter").
        """
        from vw.postgres.states import DateTrunc

        state = DateTrunc(unit=unit, expr=self.expr.state)
        return self.expr.factories.expr(state=state, factories=self.expr.factories)
