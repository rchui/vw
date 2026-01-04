"""Tests for vw.expr module."""

import vw
from vw.expr import (
    Column,
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    NotEquals,
    col,
)


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


def describe_less_than() -> None:
    """Tests for LessThan class."""

    def it_renders_less_than_comparison(render_context: vw.RenderContext) -> None:
        """Should render less than comparison with < operator."""
        less_than = LessThan(left=col("a"), right=col("b"))
        assert less_than.__vw_render__(render_context) == "a < b"


def describe_less_than_or_equal() -> None:
    """Tests for LessThanOrEqual class."""

    def it_renders_less_than_or_equal_comparison(render_context: vw.RenderContext) -> None:
        """Should render less than or equal comparison with <= operator."""
        less_than_or_equal = LessThanOrEqual(left=col("x"), right=col("y"))
        assert less_than_or_equal.__vw_render__(render_context) == "x <= y"


def describe_greater_than() -> None:
    """Tests for GreaterThan class."""

    def it_renders_greater_than_comparison(render_context: vw.RenderContext) -> None:
        """Should render greater than comparison with > operator."""
        greater_than = GreaterThan(left=col("a"), right=col("b"))
        assert greater_than.__vw_render__(render_context) == "a > b"


def describe_greater_than_or_equal() -> None:
    """Tests for GreaterThanOrEqual class."""

    def it_renders_greater_than_or_equal_comparison(render_context: vw.RenderContext) -> None:
        """Should render greater than or equal comparison with >= operator."""
        greater_than_or_equal = GreaterThanOrEqual(left=col("x"), right=col("y"))
        assert greater_than_or_equal.__vw_render__(render_context) == "x >= y"


def describe_parameter() -> None:
    """Tests for Parameter class."""

    def it_renders_parameter_with_colon_style(render_context: vw.RenderContext) -> None:
        """Should render parameter with colon prefix and register in context."""
        param = vw.param("age", 25)
        assert param.__vw_render__(render_context) == ":age"
        assert render_context.params == {"age": 25}

    def it_renders_string_parameter(render_context: vw.RenderContext) -> None:
        """Should render string parameter."""
        param = vw.param("name", "Alice")
        assert param.__vw_render__(render_context) == ":name"
        assert render_context.params == {"name": "Alice"}

    def it_renders_float_parameter(render_context: vw.RenderContext) -> None:
        """Should render float parameter."""
        param = vw.param("price", 19.99)
        assert param.__vw_render__(render_context) == ":price"
        assert render_context.params == {"price": 19.99}

    def it_renders_bool_parameter(render_context: vw.RenderContext) -> None:
        """Should render boolean parameter."""
        param = vw.param("active", True)
        assert param.__vw_render__(render_context) == ":active"
        assert render_context.params == {"active": True}

    def it_allows_reusing_same_parameter(render_context: vw.RenderContext) -> None:
        """Should allow using the same parameter multiple times."""
        param = vw.param("threshold", 100)
        result1 = param.__vw_render__(render_context)
        result2 = param.__vw_render__(render_context)
        assert result1 == ":threshold"
        assert result2 == ":threshold"
        assert render_context.params == {"threshold": 100}


def describe_param_function() -> None:
    """Tests for param() function."""

    def it_creates_parameter() -> None:
        """Should create Parameter instance."""
        param = vw.param("user_id", 42)
        assert isinstance(param, vw.Parameter)
        assert param.name == "user_id"
        assert param.value == 42

    def it_rejects_unsupported_types() -> None:
        """Should raise TypeError for unsupported value types."""
        import pytest

        with pytest.raises(TypeError, match="Unsupported parameter type: list"):
            vw.param("data", [1, 2, 3])

        with pytest.raises(TypeError, match="Unsupported parameter type: dict"):
            vw.param("config", {"key": "value"})
