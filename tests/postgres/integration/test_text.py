"""Integration tests for string scalar functions."""

from tests.utils import sql
from vw.postgres import col, param, ref, render


def describe_string_functions():
    """Test string functions via .text accessor."""

    def describe_upper():
        def it_upper_basic():
            expected_sql = """SELECT UPPER(name) FROM users"""

            q = ref("users").select(col("name").text.upper())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_upper_with_alias():
            expected_sql = """SELECT UPPER(name) AS upper_name FROM users"""

            q = ref("users").select(col("name").text.upper().alias("upper_name"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_lower():
        def it_lower_basic():
            expected_sql = """SELECT LOWER(email) FROM users"""

            q = ref("users").select(col("email").text.lower())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_lower_in_where():
            expected_sql = """SELECT email FROM users WHERE LOWER(email) = $email"""

            q = ref("users").select(col("email")).where(col("email").text.lower() == param("email", "test@example.com"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"email": "test@example.com"}

    def describe_trim():
        def it_trim_basic():
            expected_sql = """SELECT TRIM(name) FROM users"""

            q = ref("users").select(col("name").text.trim())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_ltrim():
        def it_ltrim_basic():
            expected_sql = """SELECT LTRIM(name) FROM users"""

            q = ref("users").select(col("name").text.ltrim())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_rtrim():
        def it_rtrim_basic():
            expected_sql = """SELECT RTRIM(name) FROM users"""

            q = ref("users").select(col("name").text.rtrim())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_length():
        def it_length_basic():
            expected_sql = """SELECT LENGTH(name) FROM users"""

            q = ref("users").select(col("name").text.length())
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_length_with_alias():
            expected_sql = """SELECT LENGTH(name) AS name_len FROM users"""

            q = ref("users").select(col("name").text.length().alias("name_len"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_substring():
        def it_substring_start_only():
            expected_sql = """SELECT SUBSTRING(name, 1) FROM users"""

            q = ref("users").select(col("name").text.substring(1))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_substring_start_and_length():
            expected_sql = """SELECT SUBSTRING(name, 1, 3) FROM users"""

            q = ref("users").select(col("name").text.substring(1, 3))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_replace():
        def it_replace_basic():
            expected_sql = """SELECT REPLACE(name, $old, $new) FROM users"""

            q = ref("users").select(col("name").text.replace(param("old", "foo"), param("new", "bar")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"old": "foo", "new": "bar"}

        def it_replace_with_columns():
            expected_sql = """SELECT REPLACE(description, old_val, new_val) FROM data"""

            q = ref("data").select(col("description").text.replace(col("old_val"), col("new_val")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

    def describe_concat():
        def it_concat_two():
            expected_sql = """SELECT CONCAT(first_name, last_name) FROM users"""

            q = ref("users").select(col("first_name").text.concat(col("last_name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}

        def it_concat_three():
            expected_sql = """SELECT CONCAT(first_name, $sep, last_name) FROM users"""

            q = ref("users").select(col("first_name").text.concat(param("sep", " "), col("last_name")))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {"sep": " "}

        def it_concat_with_alias():
            expected_sql = """SELECT CONCAT(first_name, last_name) AS full_name FROM users"""

            q = ref("users").select(col("first_name").text.concat(col("last_name")).alias("full_name"))
            result = render(q)
            assert result.query == sql(expected_sql)
            assert result.params == {}
