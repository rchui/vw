"""Tests for core datetime module."""

from vw.core.datetime import DateTimeAccessor
from vw.core.states import Column, CurrentDate, CurrentTime, CurrentTimestamp, Extract
from vw.postgres import F, col, render
from vw.postgres.base import Expression


def describe_datetime_accessor() -> None:
    """Test DateTimeAccessor class."""

    def it_creates_with_expression() -> None:
        """DateTimeAccessor should store the expression."""
        expr = col("created_at")
        dt = DateTimeAccessor(expr)

        assert dt.expr == expr

    def it_accessible_via_dt_property() -> None:
        """Expression should have .dt property."""
        expr = col("created_at")

        assert hasattr(expr, "dt")
        assert isinstance(expr.dt, DateTimeAccessor)
        assert expr.dt.expr == expr


def describe_extract_method() -> None:
    """Test .dt.extract() method."""

    def it_returns_expression() -> None:
        """extract() should return an Expression."""
        expr = col("created_at").dt.extract("year")

        assert isinstance(expr, Expression)

    def it_creates_extract_state() -> None:
        """extract() should create Extract state with field and expr."""
        expr = col("created_at").dt.extract("year")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "year"
        assert isinstance(expr.state.expr, Column)

    def it_extracts_year() -> None:
        """extract('year') should create Extract state for year."""
        expr = col("created_at").dt.extract("year")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "year"

    def it_extracts_month() -> None:
        """extract('month') should create Extract state for month."""
        expr = col("created_at").dt.extract("month")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "month"

    def it_extracts_day() -> None:
        """extract('day') should create Extract state for day."""
        expr = col("created_at").dt.extract("day")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "day"

    def it_extracts_hour() -> None:
        """extract('hour') should create Extract state for hour."""
        expr = col("created_at").dt.extract("hour")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "hour"

    def it_extracts_minute() -> None:
        """extract('minute') should create Extract state for minute."""
        expr = col("created_at").dt.extract("minute")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "minute"

    def it_extracts_second() -> None:
        """extract('second') should create Extract state for second."""
        expr = col("created_at").dt.extract("second")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "second"

    def it_extracts_epoch() -> None:
        """extract('epoch') should create Extract state for epoch."""
        expr = col("created_at").dt.extract("epoch")

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "epoch"

    def it_preserves_source_expression() -> None:
        """extract() should preserve the source expression."""
        source = col("timestamp_col")
        expr = source.dt.extract("year")

        assert isinstance(expr.state, Extract)
        assert isinstance(expr.state.expr, Column)
        assert expr.state.expr.name == "timestamp_col"


def describe_extract_convenience_methods() -> None:
    """Test DateTimeAccessor convenience methods."""

    def it_has_quarter_method() -> None:
        """quarter() should extract quarter field."""
        expr = col("created_at").dt.quarter()

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "quarter"

    def it_has_week_method() -> None:
        """week() should extract week field."""
        expr = col("created_at").dt.week()

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "week"

    def it_has_weekday_method() -> None:
        """weekday() should extract dow (day of week) field."""
        expr = col("created_at").dt.weekday()

        assert isinstance(expr.state, Extract)
        assert expr.state.field == "dow"


def describe_extract_rendering() -> None:
    """Test Extract state rendering."""

    def it_renders_extract_year() -> None:
        """EXTRACT(year) should render correctly."""
        expr = col("created_at").dt.extract("year")
        query = render(expr)

        assert query.query == "EXTRACT(YEAR FROM created_at)"
        assert query.params == {}

    def it_renders_extract_month() -> None:
        """EXTRACT(month) should render correctly."""
        expr = col("created_at").dt.extract("month")
        query = render(expr)

        assert query.query == "EXTRACT(MONTH FROM created_at)"
        assert query.params == {}

    def it_renders_extract_day() -> None:
        """EXTRACT(day) should render correctly."""
        expr = col("created_at").dt.extract("day")
        query = render(expr)

        assert query.query == "EXTRACT(DAY FROM created_at)"
        assert query.params == {}

    def it_renders_extract_hour() -> None:
        """EXTRACT(hour) should render correctly."""
        expr = col("created_at").dt.extract("hour")
        query = render(expr)

        assert query.query == "EXTRACT(HOUR FROM created_at)"
        assert query.params == {}

    def it_renders_extract_minute() -> None:
        """EXTRACT(minute) should render correctly."""
        expr = col("created_at").dt.extract("minute")
        query = render(expr)

        assert query.query == "EXTRACT(MINUTE FROM created_at)"
        assert query.params == {}

    def it_renders_extract_second() -> None:
        """EXTRACT(second) should render correctly."""
        expr = col("created_at").dt.extract("second")
        query = render(expr)

        assert query.query == "EXTRACT(SECOND FROM created_at)"
        assert query.params == {}

    def it_renders_extract_epoch() -> None:
        """EXTRACT(epoch) should render correctly."""
        expr = col("created_at").dt.extract("epoch")
        query = render(expr)

        assert query.query == "EXTRACT(EPOCH FROM created_at)"
        assert query.params == {}

    def it_renders_extract_quarter() -> None:
        """quarter() convenience method should render correctly."""
        expr = col("created_at").dt.quarter()
        query = render(expr)

        assert query.query == "EXTRACT(QUARTER FROM created_at)"
        assert query.params == {}

    def it_renders_extract_week() -> None:
        """week() convenience method should render correctly."""
        expr = col("created_at").dt.week()
        query = render(expr)

        assert query.query == "EXTRACT(WEEK FROM created_at)"
        assert query.params == {}

    def it_renders_extract_weekday() -> None:
        """weekday() convenience method should render correctly."""
        expr = col("created_at").dt.weekday()
        query = render(expr)

        assert query.query == "EXTRACT(DOW FROM created_at)"
        assert query.params == {}

    def it_uppercases_field_name() -> None:
        """Field name should be uppercased in SQL."""
        expr = col("created_at").dt.extract("year")
        query = render(expr)

        # Field should be YEAR not year
        assert "YEAR" in query.query
        assert "year" not in query.query

    def it_renders_with_qualified_column() -> None:
        """EXTRACT should work with qualified column names."""
        expr = col("users.created_at").dt.extract("year")
        query = render(expr)

        assert query.query == "EXTRACT(YEAR FROM users.created_at)"
        assert query.params == {}

    def it_renders_with_alias() -> None:
        """EXTRACT should support aliasing."""
        expr = col("created_at").dt.extract("year").alias("creation_year")
        query = render(expr)

        assert query.query == "EXTRACT(YEAR FROM created_at) AS creation_year"
        assert query.params == {}


def describe_extract_in_expressions() -> None:
    """Test Extract in complex expressions."""

    def it_works_in_where_clause() -> None:
        """EXTRACT can be used in WHERE conditions."""
        from vw.postgres import lit, ref

        expr = ref("events").select(col("id")).where(col("created_at").dt.extract("year") == lit(2024))
        query = render(expr)

        assert "EXTRACT(YEAR FROM created_at) = 2024" in query.query

    def it_works_in_select_list() -> None:
        """EXTRACT can be used in SELECT list."""
        from vw.postgres import ref

        expr = ref("events").select(col("id"), col("created_at").dt.extract("year").alias("year"))
        query = render(expr)

        assert "EXTRACT(YEAR FROM created_at) AS year" in query.query

    def it_works_in_comparison() -> None:
        """EXTRACT can be compared with other expressions."""
        from vw.postgres import lit

        expr = col("created_at").dt.extract("month") > lit(6)
        query = render(expr)

        assert query.query == "EXTRACT(MONTH FROM created_at) > 6"

    def it_works_in_arithmetic() -> None:
        """EXTRACT result can be used in arithmetic."""
        from vw.postgres import lit

        expr = col("created_at").dt.extract("year") + lit(1)
        query = render(expr)

        assert query.query == "EXTRACT(YEAR FROM created_at) + 1"


def describe_current_timestamp_function() -> None:
    """Test F.current_timestamp() function."""

    def it_returns_expression() -> None:
        """current_timestamp() should return an Expression."""
        expr = F.current_timestamp()

        assert isinstance(expr, Expression)

    def it_creates_current_timestamp_state() -> None:
        """current_timestamp() should create CurrentTimestamp state."""
        expr = F.current_timestamp()

        assert isinstance(expr.state, CurrentTimestamp)

    def it_is_expr_subclass() -> None:
        """CurrentTimestamp should inherit from Expr."""
        from vw.core.states import Expr

        state = CurrentTimestamp()

        assert isinstance(state, Expr)

    def it_is_frozen() -> None:
        """CurrentTimestamp should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        state = CurrentTimestamp()

        # CurrentTimestamp has no fields, but it should still be frozen
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            state.new_field = "value"  # type: ignore


def describe_current_timestamp_rendering() -> None:
    """Test CurrentTimestamp state rendering."""

    def it_renders_current_timestamp() -> None:
        """CURRENT_TIMESTAMP should render correctly."""
        expr = F.current_timestamp()
        query = render(expr)

        assert query.query == "CURRENT_TIMESTAMP"
        assert query.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_TIMESTAMP should support aliasing."""
        expr = F.current_timestamp().alias("now")
        query = render(expr)

        assert query.query == "CURRENT_TIMESTAMP AS now"
        assert query.params == {}


def describe_current_timestamp_in_expressions() -> None:
    """Test CurrentTimestamp in complex expressions."""

    def it_works_in_select_list() -> None:
        """CURRENT_TIMESTAMP can be used in SELECT list."""
        from vw.postgres import ref

        expr = ref("events").select(col("id"), F.current_timestamp().alias("fetched_at"))
        query = render(expr)

        assert "CURRENT_TIMESTAMP AS fetched_at" in query.query

    def it_works_in_comparison() -> None:
        """CURRENT_TIMESTAMP can be compared with columns."""
        expr = col("created_at") < F.current_timestamp()
        query = render(expr)

        assert query.query == "created_at < CURRENT_TIMESTAMP"


def describe_current_date_function() -> None:
    """Test F.current_date() function."""

    def it_returns_expression() -> None:
        """current_date() should return an Expression."""
        expr = F.current_date()

        assert isinstance(expr, Expression)

    def it_creates_current_date_state() -> None:
        """current_date() should create CurrentDate state."""
        expr = F.current_date()

        assert isinstance(expr.state, CurrentDate)

    def it_is_expr_subclass() -> None:
        """CurrentDate should inherit from Expr."""
        from vw.core.states import Expr

        state = CurrentDate()

        assert isinstance(state, Expr)

    def it_is_frozen() -> None:
        """CurrentDate should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        state = CurrentDate()

        # CurrentDate has no fields, but it should still be frozen
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            state.new_field = "value"  # type: ignore


def describe_current_date_rendering() -> None:
    """Test CurrentDate state rendering."""

    def it_renders_current_date() -> None:
        """CURRENT_DATE should render correctly."""
        expr = F.current_date()
        query = render(expr)

        assert query.query == "CURRENT_DATE"
        assert query.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_DATE should support aliasing."""
        expr = F.current_date().alias("today")
        query = render(expr)

        assert query.query == "CURRENT_DATE AS today"
        assert query.params == {}


def describe_current_date_in_expressions() -> None:
    """Test CurrentDate in complex expressions."""

    def it_works_in_select_list() -> None:
        """CURRENT_DATE can be used in SELECT list."""
        from vw.postgres import ref

        expr = ref("events").select(col("id"), F.current_date().alias("today"))
        query = render(expr)

        assert "CURRENT_DATE AS today" in query.query

    def it_works_in_comparison() -> None:
        """CURRENT_DATE can be compared with columns."""
        expr = col("event_date") == F.current_date()
        query = render(expr)

        assert query.query == "event_date = CURRENT_DATE"

    def it_works_in_where_clause() -> None:
        """CURRENT_DATE can be used in WHERE conditions."""
        from vw.postgres import ref

        expr = ref("events").select(col("id")).where(col("event_date") >= F.current_date())
        query = render(expr)

        assert "event_date >= CURRENT_DATE" in query.query


def describe_current_time_function() -> None:
    """Test F.current_time() function."""

    def it_returns_expression() -> None:
        """current_time() should return an Expression."""
        expr = F.current_time()

        assert isinstance(expr, Expression)

    def it_creates_current_time_state() -> None:
        """current_time() should create CurrentTime state."""
        expr = F.current_time()

        assert isinstance(expr.state, CurrentTime)

    def it_is_expr_subclass() -> None:
        """CurrentTime should inherit from Expr."""
        from vw.core.states import Expr

        state = CurrentTime()

        assert isinstance(state, Expr)

    def it_is_frozen() -> None:
        """CurrentTime should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        state = CurrentTime()

        # CurrentTime has no fields, but it should still be frozen
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            state.new_field = "value"  # type: ignore


def describe_current_time_rendering() -> None:
    """Test CurrentTime state rendering."""

    def it_renders_current_time() -> None:
        """CURRENT_TIME should render correctly."""
        expr = F.current_time()
        query = render(expr)

        assert query.query == "CURRENT_TIME"
        assert query.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_TIME should support aliasing."""
        expr = F.current_time().alias("now_time")
        query = render(expr)

        assert query.query == "CURRENT_TIME AS now_time"
        assert query.params == {}


def describe_current_time_in_expressions() -> None:
    """Test CurrentTime in complex expressions."""

    def it_works_in_select_list() -> None:
        """CURRENT_TIME can be used in SELECT list."""
        from vw.postgres import ref

        expr = ref("events").select(col("id"), F.current_time().alias("time_now"))
        query = render(expr)

        assert "CURRENT_TIME AS time_now" in query.query

    def it_works_in_comparison() -> None:
        """CURRENT_TIME can be compared with columns."""
        expr = col("event_time") > F.current_time()
        query = render(expr)

        assert query.query == "event_time > CURRENT_TIME"


def describe_extract_state() -> None:
    """Test Extract state dataclass."""

    def it_creates_with_field_and_expr() -> None:
        """Extract should store field and expression."""
        col_state = Column(name="created_at")
        extract_state = Extract(field="year", expr=col_state)

        assert extract_state.field == "year"
        assert extract_state.expr == col_state

    def it_is_frozen() -> None:
        """Extract should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        col_state = Column(name="created_at")
        extract_state = Extract(field="year", expr=col_state)

        with pytest.raises(dataclasses.FrozenInstanceError):
            extract_state.field = "month"  # type: ignore

    def it_is_expr_subclass() -> None:
        """Extract should inherit from Expr base class."""
        from vw.core.states import Expr

        col_state = Column(name="created_at")
        extract_state = Extract(field="year", expr=col_state)

        assert isinstance(extract_state, Expr)


def describe_datetime_states_isolation() -> None:
    """Test that datetime states are independent."""

    def it_creates_independent_current_timestamp_instances() -> None:
        """Multiple current_timestamp() calls should create independent states."""
        expr1 = F.current_timestamp()
        expr2 = F.current_timestamp()

        # States should be different instances
        assert expr1.state is not expr2.state
        # But both should be CurrentTimestamp
        assert isinstance(expr1.state, CurrentTimestamp)
        assert isinstance(expr2.state, CurrentTimestamp)

    def it_creates_independent_current_date_instances() -> None:
        """Multiple current_date() calls should create independent states."""
        expr1 = F.current_date()
        expr2 = F.current_date()

        # States should be different instances
        assert expr1.state is not expr2.state
        # But both should be CurrentDate
        assert isinstance(expr1.state, CurrentDate)
        assert isinstance(expr2.state, CurrentDate)

    def it_creates_independent_current_time_instances() -> None:
        """Multiple current_time() calls should create independent states."""
        expr1 = F.current_time()
        expr2 = F.current_time()

        # States should be different instances
        assert expr1.state is not expr2.state
        # But both should be CurrentTime
        assert isinstance(expr1.state, CurrentTime)
        assert isinstance(expr2.state, CurrentTime)

    def it_creates_independent_extract_instances() -> None:
        """Multiple extract() calls should create independent states."""
        expr1 = col("created_at").dt.extract("year")
        expr2 = col("created_at").dt.extract("year")

        # States should be different instances
        assert expr1.state is not expr2.state
        # But both should be Extract
        assert isinstance(expr1.state, Extract)
        assert isinstance(expr2.state, Extract)
