"""Integration tests for string functions."""

import vw
from tests.utils import sql


def describe_string_functions():
    """Tests for string function operations."""

    def it_generates_upper(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT UPPER(name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.upper()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_lower(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT LOWER(email) FROM users
        """
        result = vw.Source(name="users").select(vw.col("email").str.lower()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_trim(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT TRIM(name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.trim()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_ltrim(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT LTRIM(name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.ltrim()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_rtrim(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT RTRIM(name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.rtrim()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_length(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT LENGTH(name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.length()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_substring_with_length(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT SUBSTRING(name, 1, 5) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.substring(1, 5)).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_substring_without_length(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT SUBSTRING(name, 3) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.substring(3)).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_replace(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT REPLACE(text, 'foo', 'bar') FROM documents
        """
        result = (
            vw.Source(name="documents").select(vw.col("text").str.replace("foo", "bar")).render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_concat(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CONCAT(first_name, last_name) FROM users
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("first_name").str.concat(vw.col("last_name")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_concat_multiple(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CONCAT(first_name, ' ', last_name) FROM users
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("first_name").str.concat(vw.col("' '"), vw.col("last_name")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_string_chaining():
    """Tests for chaining string operations."""

    def it_chains_trim_and_upper(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT UPPER(TRIM(name)) FROM users
        """
        result = vw.Source(name="users").select(vw.col("name").str.trim().str.upper()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_chains_lower_and_trim(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT TRIM(LOWER(email)) FROM users
        """
        result = vw.Source(name="users").select(vw.col("email").str.lower().str.trim()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_string_with_alias():
    """Tests for string functions with aliases."""

    def it_generates_upper_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT UPPER(name) AS upper_name FROM users
        """
        result = (
            vw.Source(name="users").select(vw.col("name").str.upper().alias("upper_name")).render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_string_in_where():
    """Tests for string functions in WHERE clauses."""

    def it_uses_upper_in_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (UPPER(name) = 'JOHN')
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("name").str.upper() == vw.col("'JOHN'"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_uses_length_in_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (LENGTH(name) > 5)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("name").str.length() > vw.col("5"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_string_with_parameters():
    """Tests for string functions with parameters."""

    def it_uses_parameter_with_upper(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (UPPER(name) = UPPER(:search))
        """
        search = vw.param("search", "john")
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("name").str.upper() == search.str.upper())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"search": "john"})
