"""Tests for vw.expr module."""

from vw.expr import Column, Equals, NotEquals, col


def describe_column():
    """Tests for Column class."""

    def it_renders_column_name():
        """Should render Column as its name."""
        column = Column("username")
        assert column.__vw_render__() == "username"

    def it_renders_star():
        """Should render star for wildcard."""
        column = Column("*")
        assert column.__vw_render__() == "*"

    def describe_escape_hatch():
        """Tests for SQL string escape hatch."""

        def it_renders_star_replace():
            """Should render star REPLACE extension."""
            column = Column("* REPLACE (foo AS bar)")
            assert column.__vw_render__() == "* REPLACE (foo AS bar)"

        def it_renders_star_exclude():
            """Should render star EXCLUDE extension."""
            column = Column("* EXCLUDE (foo, bar)")
            assert column.__vw_render__() == "* EXCLUDE (foo, bar)"

        def it_renders_complex_expression():
            """Should allow any SQL expression as escape hatch."""
            column = Column("CAST(price AS DECIMAL(10,2))")
            assert column.__vw_render__() == "CAST(price AS DECIMAL(10,2))"

    def describe_comparison_operators():
        """Tests for comparison operators."""

        def it_creates_equals_with_eq_operator():
            """Should create Equals expression with == operator."""
            left = col("users.id")
            right = col("orders.user_id")
            result = left == right
            assert isinstance(result, Equals)
            assert result.__vw_render__() == "users.id = orders.user_id"

        def it_creates_not_equals_with_ne_operator():
            """Should create NotEquals expression with != operator."""
            left = col("status")
            right = col("'active'")
            result = left != right
            assert isinstance(result, NotEquals)
            assert result.__vw_render__() == "status != 'active'"


def describe_col_function():
    """Tests for col() function."""

    def it_creates_column():
        """Should create a Column instance."""
        column = col("id")
        assert isinstance(column, Column)

    def it_supports_escape_hatch():
        """Should support full SQL strings via escape hatch."""
        column = col("* REPLACE (old_name AS new_name)")
        assert column.__vw_render__() == "* REPLACE (old_name AS new_name)"


def describe_equals():
    """Tests for Equals class."""

    def it_renders_equality_comparison():
        """Should render equality comparison with = operator."""
        equals = Equals(left=col("a"), right=col("b"))
        assert equals.__vw_render__() == "a = b"


def describe_not_equals():
    """Tests for NotEquals class."""

    def it_renders_inequality_comparison():
        """Should render inequality comparison with != operator."""
        not_equals = NotEquals(left=col("x"), right=col("y"))
        assert not_equals.__vw_render__() == "x != y"
