"""Tests for PostgreSQL-specific rendering in vw/postgres."""

from vw.postgres import col, interval, param, ref, render
from vw.postgres.public import F


def describe_now_rendering() -> None:
    """Test NOW() function rendering."""

    def it_renders_now_function() -> None:
        """Now state should render as NOW()."""
        expr = F.now()
        query = ref("test").select(expr.alias("current_time"))

        result = render(query)
        assert result.query == "SELECT NOW() AS current_time FROM test"
        assert result.params == {}

    def it_renders_now_in_where() -> None:
        """NOW() should work in WHERE clause."""
        query = ref("events").select(col("id")).where(col("created_at") < F.now())

        result = render(query)
        assert result.query == "SELECT id FROM events WHERE created_at < NOW()"
        assert result.params == {}


def describe_interval_rendering() -> None:
    """Test INTERVAL literal rendering."""

    def it_renders_interval_day() -> None:
        """Interval should render as INTERVAL 'amount unit'."""
        expr = interval(1, "day")
        query = ref("test").select(expr.alias("one_day"))

        result = render(query)
        assert result.query == "SELECT INTERVAL '1 day' AS one_day FROM test"
        assert result.params == {}

    def it_renders_interval_hour() -> None:
        """Interval with hour unit."""
        expr = interval(2, "hour")
        query = ref("test").select(expr.alias("two_hours"))

        result = render(query)
        assert result.query == "SELECT INTERVAL '2 hour' AS two_hours FROM test"
        assert result.params == {}

    def it_renders_interval_with_float() -> None:
        """Interval with float amount."""
        expr = interval(1.5, "day")
        query = ref("test").select(expr.alias("day_and_half"))

        result = render(query)
        assert result.query == "SELECT INTERVAL '1.5 day' AS day_and_half FROM test"
        assert result.params == {}

    def it_renders_interval_addition() -> None:
        """Interval in addition expression."""
        query = ref("events").select((col("created_at") + interval(1, "day")).alias("next_day"))

        result = render(query)
        assert result.query == "SELECT created_at + INTERVAL '1 day' AS next_day FROM events"
        assert result.params == {}

    def it_renders_interval_subtraction() -> None:
        """Interval in subtraction expression."""
        query = ref("events").select((col("expires_at") - interval(30, "day")).alias("grace_period"))

        result = render(query)
        assert result.query == "SELECT expires_at - INTERVAL '30 day' AS grace_period FROM events"
        assert result.params == {}

    def it_renders_interval_in_where() -> None:
        """Interval in WHERE clause."""
        query = ref("events").select(col("id")).where(col("created_at") > (F.now() - interval(7, "day")))

        result = render(query)
        assert result.query == "SELECT id FROM events WHERE created_at > NOW() - INTERVAL '7 day'"
        assert result.params == {}


def describe_date_trunc_rendering() -> None:
    """Test DATE_TRUNC function rendering."""

    def it_renders_date_trunc() -> None:
        """DateTrunc should render as DATE_TRUNC('unit', expr)."""
        expr = col("created_at").dt.date_trunc("day")
        query = ref("events").select(expr.alias("day"))

        result = render(query)
        assert result.query == "SELECT DATE_TRUNC('day', created_at) AS day FROM events"
        assert result.params == {}

    def it_renders_date_trunc_hour() -> None:
        """DateTrunc with hour unit."""
        expr = col("created_at").dt.date_trunc("hour")
        query = ref("events").select(expr.alias("hour"))

        result = render(query)
        assert result.query == "SELECT DATE_TRUNC('hour', created_at) AS hour FROM events"
        assert result.params == {}

    def it_renders_date_trunc_month() -> None:
        """DateTrunc with month unit."""
        expr = col("created_at").dt.date_trunc("month")
        query = ref("events").select(expr.alias("month"))

        result = render(query)
        assert result.query == "SELECT DATE_TRUNC('month', created_at) AS month FROM events"
        assert result.params == {}

    def it_renders_date_trunc_in_group_by() -> None:
        """DateTrunc in GROUP BY clause."""
        day = col("created_at").dt.date_trunc("day")
        query = ref("events").select(day.alias("day"), F.count(col("*")).alias("total")).group_by(day)

        result = render(query)
        assert result.query == (
            "SELECT DATE_TRUNC('day', created_at) AS day, COUNT(*) AS total "
            "FROM events GROUP BY DATE_TRUNC('day', created_at)"
        )
        assert result.params == {}


def describe_ilike_rendering() -> None:
    """Test ILIKE (case-insensitive LIKE) rendering."""

    def it_renders_ilike() -> None:
        """ILike should render as ILIKE."""
        query = ref("users").select(col("name")).where(col("name").ilike(param("pattern", "john%")))

        result = render(query)
        assert result.query == "SELECT name FROM users WHERE name ILIKE $pattern"
        assert result.params == {"pattern": "john%"}

    def it_renders_not_ilike() -> None:
        """NotILike should render as NOT ILIKE."""
        query = ref("users").select(col("name")).where(col("name").not_ilike(param("pattern", "admin%")))

        result = render(query)
        assert result.query == "SELECT name FROM users WHERE name NOT ILIKE $pattern"
        assert result.params == {"pattern": "admin%"}

    def it_renders_ilike_with_literal() -> None:
        """ILIKE with literal pattern."""
        from vw.postgres import lit

        query = ref("users").select(col("name")).where(col("email").ilike(lit("%@gmail.com")))

        result = render(query)
        assert result.query == "SELECT name FROM users WHERE email ILIKE '%@gmail.com'"
        assert result.params == {}


def describe_rollup_rendering() -> None:
    """Test ROLLUP grouping construct rendering."""

    def it_renders_rollup() -> None:
        """Rollup should render as ROLLUP (columns)."""
        from vw.postgres import rollup

        query = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(rollup(col("region"), col("product")))
        )

        result = render(query)
        assert result.query == (
            "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region, product)"
        )
        assert result.params == {}

    def it_renders_rollup_single_column() -> None:
        """Rollup with single column."""
        from vw.postgres import rollup

        query = ref("sales").select(col("region"), F.sum(col("amount")).alias("total")).group_by(rollup(col("region")))

        result = render(query)
        assert result.query == "SELECT region, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region)"
        assert result.params == {}


def describe_cube_rendering() -> None:
    """Test CUBE grouping construct rendering."""

    def it_renders_cube() -> None:
        """Cube should render as CUBE (columns)."""
        from vw.postgres import cube

        query = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(cube(col("region"), col("product")))
        )

        result = render(query)
        assert result.query == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE (region, product)"
        assert result.params == {}

    def it_renders_cube_three_columns() -> None:
        """Cube with three columns."""
        from vw.postgres import cube

        query = (
            ref("sales")
            .select(col("year"), col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(cube(col("year"), col("region"), col("product")))
        )

        result = render(query)
        assert result.query == (
            "SELECT year, region, product, SUM(amount) AS total FROM sales GROUP BY CUBE (year, region, product)"
        )
        assert result.params == {}


def describe_grouping_sets_rendering() -> None:
    """Test GROUPING SETS construct rendering."""

    def it_renders_grouping_sets() -> None:
        """GroupingSets should render as GROUPING SETS ((set1), (set2), ...)."""
        from vw.postgres import grouping_sets

        query = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
        )

        result = render(query)
        assert result.query == (
            "SELECT region, product, SUM(amount) AS total FROM sales "
            "GROUP BY GROUPING SETS ((region, product), (region), ())"
        )
        assert result.params == {}

    def it_renders_grouping_sets_with_empty_set() -> None:
        """GroupingSets with empty set for grand total."""
        from vw.postgres import grouping_sets

        query = ref("sales").select(F.sum(col("amount")).alias("total")).group_by(grouping_sets(()))

        result = render(query)
        assert result.query == "SELECT SUM(amount) AS total FROM sales GROUP BY GROUPING SETS (())"
        assert result.params == {}

    def it_renders_grouping_sets_complex() -> None:
        """GroupingSets with multiple combinations."""
        from vw.postgres import grouping_sets

        query = (
            ref("sales")
            .select(col("year"), col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(
                grouping_sets(
                    (col("year"), col("region"), col("product")),
                    (col("year"), col("region")),
                    (col("year"),),
                    (),
                )
            )
        )

        result = render(query)
        assert result.query == (
            "SELECT year, region, product, SUM(amount) AS total FROM sales "
            "GROUP BY GROUPING SETS ((year, region, product), (year, region), (year), ())"
        )
        assert result.params == {}
