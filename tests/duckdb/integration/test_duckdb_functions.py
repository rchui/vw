"""Integration tests for DuckDB-specific functions (Phase 8)."""

from vw.duckdb import F, col, lit, ref, render


def describe_string_functions():
    """Test DuckDB-specific string functions."""

    def describe_regexp_matches():
        """Test REGEXP_MATCHES function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.regexp_matches(col("email"), lit(r".*@.*\.com")).alias("is_dotcom"))
            result = render(query)
            assert result.query == r"SELECT REGEXP_MATCHES(email, '.*@.*\.com') AS is_dotcom FROM t"
            assert result.params == {}

        def it_renders_in_where_clause():
            query = ref("t").select(col("id")).where(F.regexp_matches(col("phone"), lit(r"\d{10}")))
            result = render(query)
            assert result.query == r"SELECT id FROM t WHERE REGEXP_MATCHES(phone, '\d{10}')"
            assert result.params == {}

    def describe_regexp_replace():
        """Test REGEXP_REPLACE function."""

        def it_renders_without_flags():
            query = ref("t").select(F.regexp_replace(col("text"), lit(r"\s+"), lit(" ")).alias("cleaned"))
            result = render(query)
            assert result.query == r"SELECT REGEXP_REPLACE(text, '\s+', ' ') AS cleaned FROM t"
            assert result.params == {}

        def it_renders_with_flags():
            query = ref("t").select(
                F.regexp_replace(col("text"), lit(r"[aeiou]"), lit("*"), lit("g")).alias("vowels_replaced")
            )
            result = render(query)
            assert result.query == r"SELECT REGEXP_REPLACE(text, '[aeiou]', '*', 'g') AS vowels_replaced FROM t"
            assert result.params == {}

    def describe_regexp_extract():
        """Test REGEXP_EXTRACT function."""

        def it_renders_without_group():
            query = ref("t").select(F.regexp_extract(col("text"), lit(r"\d+")).alias("digits"))
            result = render(query)
            assert result.query == r"SELECT REGEXP_EXTRACT(text, '\d+') AS digits FROM t"
            assert result.params == {}

        def it_renders_with_group():
            query = ref("t").select(F.regexp_extract(col("text"), lit(r"(\w+)@(\w+)"), lit(1)).alias("user_part"))
            result = render(query)
            assert result.query == r"SELECT REGEXP_EXTRACT(text, '(\w+)@(\w+)', 1) AS user_part FROM t"
            assert result.params == {}

    def describe_string_split():
        """Test STRING_SPLIT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.string_split(col("tags"), lit(",")).alias("tag_list"))
            result = render(query)
            assert result.query == "SELECT STRING_SPLIT(tags, ',') AS tag_list FROM t"
            assert result.params == {}

        def it_renders_path_split():
            query = ref("t").select(F.string_split(col("path"), lit("/")).alias("parts"))
            result = render(query)
            assert result.query == "SELECT STRING_SPLIT(path, '/') AS parts FROM t"
            assert result.params == {}

    def describe_string_split_regex():
        """Test STRING_SPLIT_REGEX function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.string_split_regex(col("text"), lit(r"\s+")).alias("words"))
            result = render(query)
            assert result.query == r"SELECT STRING_SPLIT_REGEX(text, '\s+') AS words FROM t"
            assert result.params == {}

        def it_renders_with_complex_regex():
            query = ref("t").select(F.string_split_regex(col("csv"), lit(r",\s*")).alias("fields"))
            result = render(query)
            assert result.query == r"SELECT STRING_SPLIT_REGEX(csv, ',\s*') AS fields FROM t"
            assert result.params == {}


def describe_datetime_functions():
    """Test DuckDB-specific date/time functions."""

    def describe_date_diff():
        """Test DATE_DIFF function."""

        def it_renders_day_diff():
            query = ref("t").select(F.date_diff(lit("day"), col("start_date"), col("end_date")).alias("days"))
            result = render(query)
            assert result.query == "SELECT DATE_DIFF('day', start_date, end_date) AS days FROM t"
            assert result.params == {}

        def it_renders_month_diff():
            query = ref("t").select(F.date_diff(lit("month"), col("hired_at"), col("left_at")).alias("months"))
            result = render(query)
            assert result.query == "SELECT DATE_DIFF('month', hired_at, left_at) AS months FROM t"
            assert result.params == {}

    def describe_date_part():
        """Test DATE_PART function."""

        def it_renders_year_part():
            query = ref("t").select(F.date_part(lit("year"), col("created_at")).alias("year"))
            result = render(query)
            assert result.query == "SELECT DATE_PART('year', created_at) AS year FROM t"
            assert result.params == {}

        def it_renders_dow_part():
            query = ref("t").select(F.date_part(lit("dow"), col("event_date")).alias("day_of_week"))
            result = render(query)
            assert result.query == "SELECT DATE_PART('dow', event_date) AS day_of_week FROM t"
            assert result.params == {}

    def describe_make_date():
        """Test MAKE_DATE function."""

        def it_renders_with_literals():
            query = ref("t").select(F.make_date(lit(2024), lit(1), lit(15)).alias("d"))
            result = render(query)
            assert result.query == "SELECT MAKE_DATE(2024, 1, 15) AS d FROM t"
            assert result.params == {}

        def it_renders_with_columns():
            query = ref("t").select(F.make_date(col("year"), col("month"), col("day")).alias("date"))
            result = render(query)
            assert result.query == "SELECT MAKE_DATE(year, month, day) AS date FROM t"
            assert result.params == {}

    def describe_make_time():
        """Test MAKE_TIME function."""

        def it_renders_with_literals():
            query = ref("t").select(F.make_time(lit(14), lit(30), lit(0)).alias("t"))
            result = render(query)
            assert result.query == "SELECT MAKE_TIME(14, 30, 0) AS t FROM t"
            assert result.params == {}

        def it_renders_with_columns():
            query = ref("t").select(F.make_time(col("hour"), col("minute"), col("second")).alias("time"))
            result = render(query)
            assert result.query == "SELECT MAKE_TIME(hour, minute, second) AS time FROM t"
            assert result.params == {}

    def describe_make_timestamp():
        """Test MAKE_TIMESTAMP function."""

        def it_renders_with_literals():
            query = ref("t").select(F.make_timestamp(lit(2024), lit(1), lit(15), lit(14), lit(30), lit(0)).alias("ts"))
            result = render(query)
            assert result.query == "SELECT MAKE_TIMESTAMP(2024, 1, 15, 14, 30, 0) AS ts FROM t"
            assert result.params == {}

        def it_renders_with_columns():
            query = ref("t").select(
                F.make_timestamp(col("yr"), col("mo"), col("dy"), col("hr"), col("mi"), col("se")).alias("ts")
            )
            result = render(query)
            assert result.query == "SELECT MAKE_TIMESTAMP(yr, mo, dy, hr, mi, se) AS ts FROM t"
            assert result.params == {}


def describe_statistical_functions():
    """Test DuckDB statistical aggregate functions."""

    def describe_approx_count_distinct():
        """Test APPROX_COUNT_DISTINCT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.approx_count_distinct(col("user_id")).alias("approx_users"))
            result = render(query)
            assert result.query == "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM t"
            assert result.params == {}

        def it_renders_in_group_by():
            query = (
                ref("t")
                .select(col("category"), F.approx_count_distinct(col("user_id")).alias("approx_users"))
                .group_by(col("category"))
            )
            result = render(query)
            assert result.query == (
                "SELECT category, APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM t GROUP BY category"
            )
            assert result.params == {}

    def describe_approx_quantile():
        """Test APPROX_QUANTILE function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.approx_quantile(col("latency"), lit(0.95)).alias("p95"))
            result = render(query)
            assert result.query == "SELECT APPROX_QUANTILE(latency, 0.95) AS p95 FROM t"
            assert result.params == {}

    def describe_mode():
        """Test MODE function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.mode(col("category")).alias("most_common"))
            result = render(query)
            assert result.query == "SELECT MODE(category) AS most_common FROM t"
            assert result.params == {}

        def it_renders_in_group_by():
            query = ref("t").select(col("dept"), F.mode(col("status")).alias("common_status")).group_by(col("dept"))
            result = render(query)
            assert result.query == "SELECT dept, MODE(status) AS common_status FROM t GROUP BY dept"
            assert result.params == {}

    def describe_median():
        """Test MEDIAN function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.median(col("age")).alias("median_age"))
            result = render(query)
            assert result.query == "SELECT MEDIAN(age) AS median_age FROM t"
            assert result.params == {}

        def it_renders_in_group_by():
            query = ref("t").select(col("dept"), F.median(col("salary")).alias("median_salary")).group_by(col("dept"))
            result = render(query)
            assert result.query == "SELECT dept, MEDIAN(salary) AS median_salary FROM t GROUP BY dept"
            assert result.params == {}

    def describe_quantile():
        """Test QUANTILE function."""

        def it_renders_p99():
            query = ref("t").select(F.quantile(col("response_time"), lit(0.99)).alias("p99"))
            result = render(query)
            assert result.query == "SELECT QUANTILE(response_time, 0.99) AS p99 FROM t"
            assert result.params == {}

        def it_renders_p50():
            query = ref("t").select(F.quantile(col("score"), lit(0.5)).alias("p50"))
            result = render(query)
            assert result.query == "SELECT QUANTILE(score, 0.5) AS p50 FROM t"
            assert result.params == {}

    def describe_reservoir_quantile():
        """Test RESERVOIR_QUANTILE function."""

        def it_renders_p95():
            query = ref("t").select(F.reservoir_quantile(col("value"), lit(0.95)).alias("p95"))
            result = render(query)
            assert result.query == "SELECT RESERVOIR_QUANTILE(value, 0.95) AS p95 FROM t"
            assert result.params == {}

        def it_renders_median():
            query = ref("t").select(F.reservoir_quantile(col("latency"), lit(0.5)).alias("p50"))
            result = render(query)
            assert result.query == "SELECT RESERVOIR_QUANTILE(latency, 0.5) AS p50 FROM t"
            assert result.params == {}


def describe_function_composition():
    """Test composing Phase 8 functions with other query features."""

    def it_renders_regexp_matches_in_where():
        query = (
            ref("users").select(col("id"), col("email")).where(F.regexp_matches(col("email"), lit(r".*@company\.com")))
        )
        result = render(query)
        assert result.query == r"SELECT id, email FROM users WHERE REGEXP_MATCHES(email, '.*@company\.com')"
        assert result.params == {}

    def it_renders_median_with_having():
        query = (
            ref("orders")
            .select(col("customer_id"), F.median(col("amount")).alias("median_amount"))
            .group_by(col("customer_id"))
            .having(F.median(col("amount")) > lit(100))
        )
        result = render(query)
        assert result.query == (
            "SELECT customer_id, MEDIAN(amount) AS median_amount FROM orders "
            "GROUP BY customer_id HAVING MEDIAN(amount) > 100"
        )
        assert result.params == {}

    def it_renders_date_diff_in_where():
        query = (
            ref("events").select(col("id")).where(F.date_diff(lit("day"), col("created_at"), col("ended_at")) > lit(7))
        )
        result = render(query)
        assert result.query == "SELECT id FROM events WHERE DATE_DIFF('day', created_at, ended_at) > 7"
        assert result.params == {}

    def it_renders_string_split_with_list_count():
        query = ref("t").select(F.list_count(F.string_split(col("csv"), lit(","))).alias("field_count"))
        result = render(query)
        assert result.query == "SELECT LIST_COUNT(STRING_SPLIT(csv, ',')) AS field_count FROM t"
        assert result.params == {}
