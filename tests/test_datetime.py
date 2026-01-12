"""Unit tests for datetime functions."""

import vw
from vw.datetime import (
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    DateTimeAccessor,
    DateTrunc,
    Extract,
    Now,
    ToDate,
    ToTime,
)


def describe_datetime_accessor() -> None:
    """Tests for DateTimeAccessor."""

    def it_creates_accessor_from_column() -> None:
        expr = vw.col("created_at")
        accessor = expr.dt
        assert isinstance(accessor, DateTimeAccessor)

    def it_returns_new_accessor_each_time() -> None:
        expr = vw.col("created_at")
        assert expr.dt is not expr.dt


def describe_extract() -> None:
    """Tests for Extract expressions."""

    def it_creates_year_extract() -> None:
        result = vw.col("created_at").dt.year()
        assert isinstance(result, Extract)
        assert result.field == "year"

    def it_creates_quarter_extract() -> None:
        result = vw.col("created_at").dt.quarter()
        assert isinstance(result, Extract)
        assert result.field == "quarter"

    def it_creates_month_extract() -> None:
        result = vw.col("created_at").dt.month()
        assert isinstance(result, Extract)
        assert result.field == "month"

    def it_creates_week_extract() -> None:
        result = vw.col("created_at").dt.week()
        assert isinstance(result, Extract)
        assert result.field == "week"

    def it_creates_day_extract() -> None:
        result = vw.col("created_at").dt.day()
        assert isinstance(result, Extract)
        assert result.field == "day"

    def it_creates_hour_extract() -> None:
        result = vw.col("created_at").dt.hour()
        assert isinstance(result, Extract)
        assert result.field == "hour"

    def it_creates_minute_extract() -> None:
        result = vw.col("created_at").dt.minute()
        assert isinstance(result, Extract)
        assert result.field == "minute"

    def it_creates_second_extract() -> None:
        result = vw.col("created_at").dt.second()
        assert isinstance(result, Extract)
        assert result.field == "second"

    def it_creates_weekday_extract() -> None:
        result = vw.col("created_at").dt.weekday()
        assert isinstance(result, Extract)
        assert result.field == "weekday"


def describe_date_trunc() -> None:
    """Tests for DateTrunc expressions."""

    def it_creates_truncate_year() -> None:
        result = vw.col("created_at").dt.truncate("year")
        assert isinstance(result, DateTrunc)
        assert result.unit == "year"

    def it_creates_truncate_month() -> None:
        result = vw.col("created_at").dt.truncate("month")
        assert isinstance(result, DateTrunc)
        assert result.unit == "month"

    def it_creates_truncate_day() -> None:
        result = vw.col("created_at").dt.truncate("day")
        assert isinstance(result, DateTrunc)
        assert result.unit == "day"

    def it_creates_truncate_hour() -> None:
        result = vw.col("created_at").dt.truncate("hour")
        assert isinstance(result, DateTrunc)
        assert result.unit == "hour"


def describe_conversion() -> None:
    """Tests for date/time conversion expressions."""

    def it_creates_to_date() -> None:
        result = vw.col("created_at").dt.date()
        assert isinstance(result, ToDate)

    def it_creates_to_time() -> None:
        result = vw.col("created_at").dt.time()
        assert isinstance(result, ToTime)


def describe_standalone_functions() -> None:
    """Tests for standalone datetime functions."""

    def it_creates_current_timestamp() -> None:
        result = vw.current_timestamp()
        assert isinstance(result, CurrentTimestamp)

    def it_creates_current_date() -> None:
        result = vw.current_date()
        assert isinstance(result, CurrentDate)

    def it_creates_current_time() -> None:
        result = vw.current_time()
        assert isinstance(result, CurrentTime)

    def it_creates_now() -> None:
        result = vw.now()
        assert isinstance(result, Now)


def describe_chaining() -> None:
    """Tests for chaining datetime operations."""

    def it_chains_truncate_and_extract() -> None:
        result = vw.col("created_at").dt.truncate("month").dt.year()
        assert isinstance(result, Extract)
        assert result.field == "year"
        assert isinstance(result.expr, DateTrunc)

    def it_chains_date_and_extract() -> None:
        result = vw.col("created_at").dt.date().dt.day()
        assert isinstance(result, Extract)
        assert result.field == "day"
        assert isinstance(result.expr, ToDate)


def describe_interval() -> None:
    """Tests for Interval expressions."""

    def it_creates_interval_days() -> None:
        result = vw.interval(1, "days")
        assert result.amount == 1
        assert result.unit == "days"

    def it_creates_interval_hours() -> None:
        result = vw.interval(6, "hours")
        assert result.amount == 6
        assert result.unit == "hours"

    def it_creates_interval_with_float() -> None:
        result = vw.interval(2.5, "hours")
        assert result.amount == 2.5
        assert result.unit == "hours"

    def it_creates_interval_with_months() -> None:
        result = vw.interval(1, "months")
        assert result.amount == 1
        assert result.unit == "months"


def describe_add_interval() -> None:
    """Tests for AddInterval expressions."""

    def it_creates_add_interval_days() -> None:
        result = vw.col("created_at").dt.date_add(1, "days")
        assert result.expr == vw.col("created_at")
        assert result.amount == 1
        assert result.unit == "days"

    def it_creates_add_interval_hours() -> None:
        result = vw.col("created_at").dt.date_add(6, "hours")
        assert result.amount == 6
        assert result.unit == "hours"


def describe_subtract_interval() -> None:
    """Tests for SubtractInterval expressions."""

    def it_creates_subtract_interval_days() -> None:
        result = vw.col("created_at").dt.date_sub(1, "days")
        assert result.expr == vw.col("created_at")
        assert result.amount == 1
        assert result.unit == "days"

    def it_creates_subtract_interval_months() -> None:
        result = vw.col("created_at").dt.date_sub(3, "months")
        assert result.amount == 3
        assert result.unit == "months"


def describe_date_literal() -> None:
    """Tests for the date() literal function."""

    def it_creates_a_date_literal() -> None:
        """Creating a date literal returns a Date object."""
        result = vw.date("2023-01-01")
        assert isinstance(result, vw.datetime.Date)
        assert result.value == "2023-01-01"

    def it_renders_correctly() -> None:
        """The date literal renders to DATE 'YYYY-MM-DD'."""
        query = vw.Source(name="test").select(vw.date("2023-01-01"))
        result = query.render()
        assert result.sql == "SELECT DATE '2023-01-01' FROM test"
