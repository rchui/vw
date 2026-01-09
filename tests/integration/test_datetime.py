"""Integration tests for datetime functions."""

import pytest

import vw


@pytest.fixture
def render_config() -> vw.RenderConfig:
    return vw.RenderConfig()


def describe_extract_functions() -> None:
    """Tests for EXTRACT rendering."""

    def it_generates_extract_year(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(YEAR FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.year()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_quarter(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(QUARTER FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.quarter()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_month(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(MONTH FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.month()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_week(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(WEEK FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.week()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_day(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(DAY FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.day()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_hour(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(HOUR FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.hour()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_minute(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(MINUTE FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.minute()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_second(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(SECOND FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.second()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_extract_weekday(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(DOW FROM created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.weekday()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_date_trunc() -> None:
    """Tests for DATE_TRUNC rendering."""

    def it_generates_date_trunc_year(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DATE_TRUNC('year', created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.truncate("year")).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_date_trunc_month(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DATE_TRUNC('month', created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.truncate("month")).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_date_trunc_day(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DATE_TRUNC('day', created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.truncate("day")).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_date_trunc_hour(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DATE_TRUNC('hour', created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.truncate("hour")).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_conversion() -> None:
    """Tests for date/time conversion rendering."""

    def it_generates_date(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DATE(created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.date()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_time(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT TIME(created_at) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.time()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_standalone_functions() -> None:
    """Tests for standalone datetime function rendering."""

    def it_generates_current_timestamp(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT CURRENT_TIMESTAMP FROM events"
        result = vw.Source(name="events").select(vw.current_timestamp()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_current_date(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT CURRENT_DATE FROM events"
        result = vw.Source(name="events").select(vw.current_date()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_current_time(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT CURRENT_TIME FROM events"
        result = vw.Source(name="events").select(vw.current_time()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_now(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT NOW() FROM events"
        result = vw.Source(name="events").select(vw.now()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_datetime_with_alias() -> None:
    """Tests for datetime with aliasing."""

    def it_generates_extract_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(YEAR FROM created_at) AS year FROM events"
        result = (
            vw.Source(name="events").select(vw.col("created_at").dt.year().alias("year")).render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_current_timestamp_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT CURRENT_TIMESTAMP AS queried_at FROM events"
        result = (
            vw.Source(name="events").select(vw.current_timestamp().alias("queried_at")).render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_datetime_in_where() -> None:
    """Tests for datetime in WHERE clause."""

    def it_uses_extract_in_where(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT * FROM events WHERE (EXTRACT(YEAR FROM created_at) = 2024)"
        result = (
            vw.Source(name="events")
            .select(vw.col("*"))
            .where(vw.col("created_at").dt.year() == vw.col("2024"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_uses_current_date_in_where(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT * FROM events WHERE (DATE(created_at) = CURRENT_DATE)"
        result = (
            vw.Source(name="events")
            .select(vw.col("*"))
            .where(vw.col("created_at").dt.date() == vw.current_date())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_datetime_chaining() -> None:
    """Tests for chaining datetime operations."""

    def it_chains_truncate_and_extract(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(YEAR FROM DATE_TRUNC('month', created_at)) FROM events"
        result = (
            vw.Source(name="events")
            .select(vw.col("created_at").dt.truncate("month").dt.year())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_chains_date_and_extract(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT EXTRACT(DAY FROM DATE(created_at)) FROM events"
        result = vw.Source(name="events").select(vw.col("created_at").dt.date().dt.day()).render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})


def describe_datetime_with_parameter() -> None:
    """Tests for datetime with parameters."""

    def it_uses_parameter_with_extract(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT * FROM events WHERE (EXTRACT(YEAR FROM created_at) = :year)"
        result = (
            vw.Source(name="events")
            .select(vw.col("*"))
            .where(vw.col("created_at").dt.year() == vw.param("year", 2024))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=expected_sql, params={"year": 2024})
