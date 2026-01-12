"""SQL datetime functions and accessor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from vw.base import Expression
from vw.render import Dialect, RenderContext

# Type alias for truncation units
TruncateUnit = Literal["year", "quarter", "month", "week", "day", "hour", "minute", "second"]

# Type alias for extraction fields
ExtractField = Literal["year", "quarter", "month", "week", "day", "hour", "minute", "second", "weekday"]


@dataclass(kw_only=True, frozen=True, eq=False)
class Extract(Expression):
    """Represents an EXTRACT() function call."""

    field: ExtractField
    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        field_upper = self.field.upper()
        # WEEKDAY is often DOW (day of week) in SQL
        if field_upper == "WEEKDAY":
            field_upper = "DOW"
        return f"EXTRACT({field_upper} FROM {self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class DateTrunc(Expression):
    """Represents a DATE_TRUNC() function call."""

    unit: TruncateUnit
    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"DATE_TRUNC('{self.unit}', {self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class ToDate(Expression):
    """Represents extracting the date part from a datetime."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"DATE({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class ToTime(Expression):
    """Represents extracting the time part from a datetime."""

    expr: Expression

    def __vw_render__(self, context: RenderContext) -> str:
        return f"TIME({self.expr.__vw_render__(context)})"


@dataclass(kw_only=True, frozen=True, eq=False)
class CurrentTimestamp(Expression):
    """Represents CURRENT_TIMESTAMP."""

    def __vw_render__(self, context: RenderContext) -> str:
        return "CURRENT_TIMESTAMP"


@dataclass(kw_only=True, frozen=True, eq=False)
class CurrentDate(Expression):
    """Represents CURRENT_DATE."""

    def __vw_render__(self, context: RenderContext) -> str:
        return "CURRENT_DATE"


@dataclass(kw_only=True, frozen=True, eq=False)
class CurrentTime(Expression):
    """Represents CURRENT_TIME."""

    def __vw_render__(self, context: RenderContext) -> str:
        return "CURRENT_TIME"


@dataclass(kw_only=True, frozen=True, eq=False)
class Now(Expression):
    """Represents NOW()."""

    def __vw_render__(self, context: RenderContext) -> str:
        return "NOW()"


@dataclass(kw_only=True, frozen=True, eq=False)
class Date(Expression):
    """Represents a DATE literal."""

    value: str

    def __vw_render__(self, context: RenderContext) -> str:
        return f"DATE '{self.value}'"


@dataclass(kw_only=True, frozen=True, eq=False)
class Interval(Expression):
    """Represents an INTERVAL value for use with operators."""

    amount: int | float
    unit: str

    def __vw_render__(self, context: RenderContext) -> str:
        from vw.exceptions import UnsupportedDialectError

        if context.config.dialect == Dialect.POSTGRES:
            return f"INTERVAL '{self.amount} {self.unit}'"
        elif context.config.dialect == Dialect.SQLSERVER:
            raise UnsupportedDialectError(
                "SQL Server does not support standalone INTERVAL literals. Use date_add/date_sub methods instead."
            )
        raise UnsupportedDialectError(f"Unsupported dialect: {context.config.dialect}")


@dataclass(kw_only=True, frozen=True, eq=False)
class AddInterval(Expression):
    """Represents datetime/timestamp + interval."""

    expr: Expression
    amount: int | float
    unit: str

    def __vw_render__(self, context: RenderContext) -> str:
        from vw.exceptions import UnsupportedDialectError

        if context.config.dialect == Dialect.POSTGRES:
            return f"DATE_ADD({self.expr.__vw_render__(context)}, INTERVAL '{self.amount} {self.unit}')"
        elif context.config.dialect == Dialect.SQLSERVER:
            return f"DATEADD({self.unit}, {self.amount}, {self.expr.__vw_render__(context)})"
        raise UnsupportedDialectError(f"Unsupported dialect: {context.config.dialect}")


@dataclass(kw_only=True, frozen=True, eq=False)
class SubtractInterval(Expression):
    """Represents datetime/timestamp - interval."""

    expr: Expression
    amount: int | float
    unit: str

    def __vw_render__(self, context: RenderContext) -> str:
        from vw.exceptions import UnsupportedDialectError

        if context.config.dialect == Dialect.POSTGRES:
            return f"DATE_SUB({self.expr.__vw_render__(context)}, INTERVAL '{self.amount} {self.unit}')"
        elif context.config.dialect == Dialect.SQLSERVER:
            return f"DATEADD({self.unit}, {-self.amount}, {self.expr.__vw_render__(context)})"
        raise UnsupportedDialectError(f"Unsupported dialect: {context.config.dialect}")


class DateTimeAccessor:
    """Accessor for datetime operations on an Expression."""

    def __init__(self, expr: Expression):
        self._expr = expr

    # Extraction methods

    def year(self) -> Extract:
        """Extract the year.

        Returns:
            An Extract expression for the year.

        Example:
            >>> col("created_at").dt.year()
        """
        return Extract(field="year", expr=self._expr)

    def quarter(self) -> Extract:
        """Extract the quarter (1-4).

        Returns:
            An Extract expression for the quarter.

        Example:
            >>> col("created_at").dt.quarter()
        """
        return Extract(field="quarter", expr=self._expr)

    def month(self) -> Extract:
        """Extract the month (1-12).

        Returns:
            An Extract expression for the month.

        Example:
            >>> col("created_at").dt.month()
        """
        return Extract(field="month", expr=self._expr)

    def week(self) -> Extract:
        """Extract the week of the year.

        Returns:
            An Extract expression for the week.

        Example:
            >>> col("created_at").dt.week()
        """
        return Extract(field="week", expr=self._expr)

    def day(self) -> Extract:
        """Extract the day of the month (1-31).

        Returns:
            An Extract expression for the day.

        Example:
            >>> col("created_at").dt.day()
        """
        return Extract(field="day", expr=self._expr)

    def hour(self) -> Extract:
        """Extract the hour (0-23).

        Returns:
            An Extract expression for the hour.

        Example:
            >>> col("created_at").dt.hour()
        """
        return Extract(field="hour", expr=self._expr)

    def minute(self) -> Extract:
        """Extract the minute (0-59).

        Returns:
            An Extract expression for the minute.

        Example:
            >>> col("created_at").dt.minute()
        """
        return Extract(field="minute", expr=self._expr)

    def second(self) -> Extract:
        """Extract the second (0-59).

        Returns:
            An Extract expression for the second.

        Example:
            >>> col("created_at").dt.second()
        """
        return Extract(field="second", expr=self._expr)

    def weekday(self) -> Extract:
        """Extract the day of the week.

        Returns:
            An Extract expression for the day of week.

        Example:
            >>> col("created_at").dt.weekday()
        """
        return Extract(field="weekday", expr=self._expr)

    # Truncation

    def truncate(self, unit: TruncateUnit) -> DateTrunc:
        """Truncate to the specified unit.

        Args:
            unit: The unit to truncate to (year, quarter, month, week, day, hour, minute, second).

        Returns:
            A DateTrunc expression.

        Example:
            >>> col("created_at").dt.truncate("month")
        """
        return DateTrunc(unit=unit, expr=self._expr)

    # Conversion

    def date(self) -> ToDate:
        """Extract the date part.

        Returns:
            A ToDate expression.

        Example:
            >>> col("created_at").dt.date()
        """
        return ToDate(expr=self._expr)

    def time(self) -> ToTime:
        """Extract the time part.

        Returns:
            A ToTime expression.

        Example:
            >>> col("created_at").dt.time()
        """
        return ToTime(expr=self._expr)

    # Interval arithmetic

    def date_add(self, amount: int | float, unit: str) -> AddInterval:
        """Add an interval to this datetime expression.

        Args:
            amount: The amount to add (can be positive or negative).
            unit: The unit of time (e.g., "days", "hours", "months").
                  For SQL Server, use singular forms like "day", "hour".

        Returns:
            An AddInterval expression.

        Example:
            >>> col("created_at").dt.date_add(1, "days")
            >>> col("created_at").dt.date_add(6, "hours")
        """
        return AddInterval(expr=self._expr, amount=amount, unit=unit)

    def date_sub(self, amount: int | float, unit: str) -> SubtractInterval:
        """Subtract an interval from this datetime expression.

        Args:
            amount: The amount to subtract (can be positive or negative).
            unit: The unit of time (e.g., "days", "hours", "months").
                  For SQL Server, use singular forms like "day", "hour".

        Returns:
            A SubtractInterval expression.

        Example:
            >>> col("created_at").dt.date_sub(1, "days")
            >>> col("created_at").dt.date_sub(6, "hours")
        """
        return SubtractInterval(expr=self._expr, amount=amount, unit=unit)


# Standalone functions


def current_timestamp() -> CurrentTimestamp:
    """Return the current timestamp.

    Returns:
        A CurrentTimestamp expression.

    Example:
        >>> vw.current_timestamp()
    """
    return CurrentTimestamp()


def current_date() -> CurrentDate:
    """Return the current date.

    Returns:
        A CurrentDate expression.

    Example:
        >>> vw.current_date()
    """
    return CurrentDate()


def current_time() -> CurrentTime:
    """Return the current time.

    Returns:
        A CurrentTime expression.

    Example:
        >>> vw.current_time()
    """
    return CurrentTime()


def now() -> Now:
    """Return the current timestamp (alias for NOW()).

    Returns:
        A Now expression.

    Example:
        >>> vw.now()
    """
    return Now()


def interval(amount: int | float, unit: str) -> Interval:
    """Create an interval value for use with + and - operators.

    Args:
        amount: The amount of time for the interval.
        unit: The unit of time (e.g., "days", "hours", "months").

    Returns:
        An Interval expression.

    Example:
        >>> vw.interval(1, "days")
        >>> vw.interval(6, "hours")
    """
    return Interval(amount=amount, unit=unit)


def date(value: str) -> Date:
    """Create a date literal.

    Args:
        value: The date string in "YYYY-MM-DD" format.

    Returns:
        A Date expression.

    Example:
        >>> from vw.datetime import date
        >>> date("2023-01-01")
    """
    return Date(value=value)
