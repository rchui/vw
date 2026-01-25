"""Tests for vw/column.py module."""

import vw.reference as vw
from vw.reference.column import Column, col
from vw.reference.operators import (
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotEquals,
)


def describe_column() -> None:
    """Tests for Column class."""

    def it_renders_column_name(render_context: vw.RenderContext) -> None:
        """Should render Column as its name."""
        column = Column(name="username")
        assert column.__vw_render__(render_context) == "username"

    def it_renders_star(render_context: vw.RenderContext) -> None:
        """Should render star for wildcard."""
        column = Column(name="*")
        assert column.__vw_render__(render_context) == "*"

    def describe_escape_hatch() -> None:
        """Tests for SQL string escape hatch."""

        def it_renders_star_replace(render_context: vw.RenderContext) -> None:
            """Should render star REPLACE extension."""
            column = Column(name="* REPLACE (foo AS bar)")
            assert column.__vw_render__(render_context) == "* REPLACE (foo AS bar)"

        def it_renders_star_exclude(render_context: vw.RenderContext) -> None:
            """Should render star EXCLUDE extension."""
            column = Column(name="* EXCLUDE (foo, bar)")
            assert column.__vw_render__(render_context) == "* EXCLUDE (foo, bar)"

        def it_renders_complex_expression(render_context: vw.RenderContext) -> None:
            """Should allow any SQL expression as escape hatch."""
            column = Column(name="CAST(price AS DECIMAL(10,2))")
            assert column.__vw_render__(render_context) == "CAST(price AS DECIMAL(10,2))"

    def describe_comparison_operators() -> None:
        """Tests for comparison operators."""

        def it_creates_equals_with_eq_operator(render_context: vw.RenderContext) -> None:
            """Should create Equals expression with == operator."""
            left = col("users.id")
            right = col("orders.user_id")
            result = left == right
            assert isinstance(result, Equals)
            assert result.__vw_render__(render_context) == "users.id = orders.user_id"

        def it_creates_not_equals_with_ne_operator(render_context: vw.RenderContext) -> None:
            """Should create NotEquals expression with != operator."""
            left = col("status")
            right = col("'active'")
            result = left != right
            assert isinstance(result, NotEquals)
            assert result.__vw_render__(render_context) == "status <> 'active'"

        def it_creates_less_than_with_lt_operator(render_context: vw.RenderContext) -> None:
            """Should create LessThan expression with < operator."""
            left = col("age")
            right = col("18")
            result = left < right
            assert isinstance(result, LessThan)
            assert result.__vw_render__(render_context) == "age < 18"

        def it_creates_less_than_or_equal_with_le_operator(render_context: vw.RenderContext) -> None:
            """Should create LessThanOrEqual expression with <= operator."""
            left = col("price")
            right = col("100.00")
            result = left <= right
            assert isinstance(result, LessThanOrEqual)
            assert result.__vw_render__(render_context) == "price <= 100.00"

        def it_creates_greater_than_with_gt_operator(render_context: vw.RenderContext) -> None:
            """Should create GreaterThan expression with > operator."""
            left = col("score")
            right = col("90")
            result = left > right
            assert isinstance(result, GreaterThan)
            assert result.__vw_render__(render_context) == "score > 90"

        def it_creates_greater_than_or_equal_with_ge_operator(render_context: vw.RenderContext) -> None:
            """Should create GreaterThanOrEqual expression with >= operator."""
            left = col("quantity")
            right = col("1")
            result = left >= right
            assert isinstance(result, GreaterThanOrEqual)
            assert result.__vw_render__(render_context) == "quantity >= 1"


def describe_col_function() -> None:
    """Tests for col() function."""

    def it_creates_column() -> None:
        """Should create a Column instance."""
        column = col("id")
        assert isinstance(column, Column)

    def it_supports_escape_hatch(render_context: vw.RenderContext) -> None:
        """Should support full SQL strings via escape hatch."""
        column = col("* REPLACE (old_name AS new_name)")
        assert column.__vw_render__(render_context) == "* REPLACE (old_name AS new_name)"
