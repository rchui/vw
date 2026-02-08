"""Integration tests for date/time scalar functions."""

from tests.utils import sql
from vw.postgres import F, col, interval, param, ref, render


def describe_current_timestamp():
    def test_basic():
        expected_sql = "SELECT CURRENT_TIMESTAMP FROM events"

        q = ref("events").select(F.current_timestamp())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_with_alias():
        expected_sql = "SELECT CURRENT_TIMESTAMP AS now FROM events"

        q = ref("events").select(F.current_timestamp().alias("now"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_current_date():
    def test_basic():
        expected_sql = "SELECT CURRENT_DATE FROM events"

        q = ref("events").select(F.current_date())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_current_time():
    def test_basic():
        expected_sql = "SELECT CURRENT_TIME FROM events"

        q = ref("events").select(F.current_time())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_now():
    def test_basic():
        expected_sql = "SELECT NOW() FROM events"

        q = ref("events").select(F.now())
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_extract():
    def test_extract_year():
        expected_sql = "SELECT EXTRACT(YEAR FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("year"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_month():
        expected_sql = "SELECT EXTRACT(MONTH FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("month"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_day():
        expected_sql = "SELECT EXTRACT(DAY FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("day"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_hour():
        expected_sql = "SELECT EXTRACT(HOUR FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("hour"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_minute():
        expected_sql = "SELECT EXTRACT(MINUTE FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("minute"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_second():
        expected_sql = "SELECT EXTRACT(SECOND FROM created_at) FROM events"

        q = ref("events").select(col("created_at").dt.extract("second"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_with_alias():
        expected_sql = "SELECT EXTRACT(YEAR FROM created_at) AS year FROM events"

        q = ref("events").select(col("created_at").dt.extract("year").alias("year"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_extract_in_where():
        expected_sql = "SELECT id FROM events WHERE EXTRACT(YEAR FROM created_at) = $year"

        q = ref("events").select(col("id")).where(col("created_at").dt.extract("year") == param("year", 2024))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"year": 2024}


def describe_interval():
    def test_interval_int():
        expected_sql = "SELECT INTERVAL '1 day' FROM events"

        q = ref("events").select(interval(1, "day"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_interval_float():
        expected_sql = "SELECT INTERVAL '1.5 hour' FROM events"

        q = ref("events").select(interval(1.5, "hour"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_date_trunc():
    def test_basic():
        expected_sql = "SELECT DATE_TRUNC('month', created_at) FROM events"

        q = ref("events").select(col("created_at").dt.date_trunc("month"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_with_alias():
        expected_sql = "SELECT DATE_TRUNC('year', created_at) AS year_start FROM events"

        q = ref("events").select(col("created_at").dt.date_trunc("year").alias("year_start"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_in_group_by():
        expected_sql = (
            "SELECT DATE_TRUNC('day', created_at), COUNT(*) FROM events GROUP BY DATE_TRUNC('day', created_at)"
        )

        q = (
            ref("events")
            .select(col("created_at").dt.date_trunc("day"), F.count())
            .group_by(col("created_at").dt.date_trunc("day"))
        )
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_interval_arithmetic():
    def test_add_interval():
        expected_sql = "SELECT created_at + INTERVAL '1 day' FROM events"

        q = ref("events").select(col("created_at") + interval(1, "day"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_subtract_interval():
        expected_sql = "SELECT created_at - INTERVAL '30 day' FROM events"

        q = ref("events").select(col("created_at") - interval(30, "day"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def test_interval_in_where():
        expected_sql = "SELECT id FROM events WHERE created_at > NOW() - INTERVAL '7 day'"

        q = ref("events").select(col("id")).where(col("created_at") > F.now() - interval(7, "day"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
