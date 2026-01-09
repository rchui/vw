"""Tests for vw/strings.py module."""

import vw
from vw.column import Column
from vw.strings import Concat, Length, Lower, LTrim, Replace, RTrim, Substring, Trim, Upper


def describe_upper() -> None:
    """Tests for Upper class."""

    def it_renders_upper(render_context: vw.RenderContext) -> None:
        expr = Upper(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "UPPER(name)"


def describe_lower() -> None:
    """Tests for Lower class."""

    def it_renders_lower(render_context: vw.RenderContext) -> None:
        expr = Lower(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "LOWER(name)"


def describe_trim() -> None:
    """Tests for Trim class."""

    def it_renders_trim(render_context: vw.RenderContext) -> None:
        expr = Trim(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "TRIM(name)"


def describe_ltrim() -> None:
    """Tests for LTrim class."""

    def it_renders_ltrim(render_context: vw.RenderContext) -> None:
        expr = LTrim(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "LTRIM(name)"


def describe_rtrim() -> None:
    """Tests for RTrim class."""

    def it_renders_rtrim(render_context: vw.RenderContext) -> None:
        expr = RTrim(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "RTRIM(name)"


def describe_length() -> None:
    """Tests for Length class."""

    def it_renders_length(render_context: vw.RenderContext) -> None:
        expr = Length(expr=Column(name="name"))
        assert expr.__vw_render__(render_context) == "LENGTH(name)"


def describe_substring() -> None:
    """Tests for Substring class."""

    def it_renders_substring_with_length(render_context: vw.RenderContext) -> None:
        expr = Substring(expr=Column(name="name"), start=1, length=5)
        assert expr.__vw_render__(render_context) == "SUBSTRING(name, 1, 5)"

    def it_renders_substring_without_length(render_context: vw.RenderContext) -> None:
        expr = Substring(expr=Column(name="name"), start=3)
        assert expr.__vw_render__(render_context) == "SUBSTRING(name, 3)"


def describe_replace() -> None:
    """Tests for Replace class."""

    def it_renders_replace(render_context: vw.RenderContext) -> None:
        expr = Replace(expr=Column(name="text"), old="foo", new="bar")
        assert expr.__vw_render__(render_context) == "REPLACE(text, 'foo', 'bar')"


def describe_concat() -> None:
    """Tests for Concat class."""

    def it_renders_concat_two_expressions(render_context: vw.RenderContext) -> None:
        expr = Concat(exprs=(Column(name="first"), Column(name="last")))
        assert expr.__vw_render__(render_context) == "CONCAT(first, last)"

    def it_renders_concat_multiple_expressions(render_context: vw.RenderContext) -> None:
        expr = Concat(exprs=(Column(name="a"), Column(name="b"), Column(name="c")))
        assert expr.__vw_render__(render_context) == "CONCAT(a, b, c)"


def describe_string_accessor() -> None:
    """Tests for StringAccessor class."""

    def it_creates_upper_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.upper()
        assert isinstance(result, Upper)
        assert result.expr == col

    def it_creates_lower_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.lower()
        assert isinstance(result, Lower)
        assert result.expr == col

    def it_creates_trim_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.trim()
        assert isinstance(result, Trim)
        assert result.expr == col

    def it_creates_ltrim_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.ltrim()
        assert isinstance(result, LTrim)
        assert result.expr == col

    def it_creates_rtrim_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.rtrim()
        assert isinstance(result, RTrim)
        assert result.expr == col

    def it_creates_length_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.length()
        assert isinstance(result, Length)
        assert result.expr == col

    def it_creates_substring_via_accessor() -> None:
        col = Column(name="name")
        result = col.str.substring(1, 5)
        assert isinstance(result, Substring)
        assert result.expr == col
        assert result.start == 1
        assert result.length == 5

    def it_creates_replace_via_accessor() -> None:
        col = Column(name="text")
        result = col.str.replace("old", "new")
        assert isinstance(result, Replace)
        assert result.expr == col
        assert result.old == "old"
        assert result.new == "new"

    def it_creates_concat_via_accessor() -> None:
        col1 = Column(name="first")
        col2 = Column(name="last")
        result = col1.str.concat(col2)
        assert isinstance(result, Concat)
        assert result.exprs == (col1, col2)

    def it_works_with_parameters(render_context: vw.RenderContext) -> None:
        param = vw.param("search_term", "test")
        result = param.str.upper()
        assert isinstance(result, Upper)
        assert result.__vw_render__(render_context) == "UPPER(:search_term)"
