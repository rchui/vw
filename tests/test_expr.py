"""Tests for vw.expr module."""

import vw
from vw.expr import Column, Equals, NotEquals, col


def describe_column() -> None:
    """Tests for Column class."""

    def it_renders_column_name(render_context: vw.RenderContext) -> None:
        """Should render Column as its name."""
        column = Column("username")
        assert column.__vw_render__(render_context) == "username"

    def it_renders_star(render_context: vw.RenderContext) -> None:
        """Should render star for wildcard."""
        column = Column("*")
        assert column.__vw_render__(render_context) == "*"

    def describe_escape_hatch() -> None:
        """Tests for SQL string escape hatch."""

        def it_renders_star_replace(render_context: vw.RenderContext) -> None:
            """Should render star REPLACE extension."""
            column = Column("* REPLACE (foo AS bar)")
            assert column.__vw_render__(render_context) == "* REPLACE (foo AS bar)"

        def it_renders_star_exclude(render_context: vw.RenderContext) -> None:
            """Should render star EXCLUDE extension."""
            column = Column("* EXCLUDE (foo, bar)")
            assert column.__vw_render__(render_context) == "* EXCLUDE (foo, bar)"

        def it_renders_complex_expression(render_context: vw.RenderContext) -> None:
            """Should allow any SQL expression as escape hatch."""
            column = Column("CAST(price AS DECIMAL(10,2))")
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
            assert result.__vw_render__(render_context) == "status != 'active'"


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


def describe_equals() -> None:
    """Tests for Equals class."""

    def it_renders_equality_comparison(render_context: vw.RenderContext) -> None:
        """Should render equality comparison with = operator."""
        equals = Equals(left=col("a"), right=col("b"))
        assert equals.__vw_render__(render_context) == "a = b"


def describe_not_equals() -> None:
    """Tests for NotEquals class."""

    def it_renders_inequality_comparison(render_context: vw.RenderContext) -> None:
        """Should render inequality comparison with != operator."""
        not_equals = NotEquals(left=col("x"), right=col("y"))
        assert not_equals.__vw_render__(render_context) == "x != y"
