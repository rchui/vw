"""Tests for vw/operators.py module."""

import vw
from vw.column import col
from vw.operators import (
    And,
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEquals,
    Or,
)


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
        assert not_equals.__vw_render__(render_context) == "x <> y"


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


def describe_and() -> None:
    """Tests for And class."""

    def it_renders_and_expression(render_context: vw.RenderContext) -> None:
        """Should render AND expression."""
        and_expr = And(
            left=Equals(left=col("a"), right=col("b")),
            right=GreaterThan(left=col("c"), right=col("d")),
        )
        assert and_expr.__vw_render__(render_context) == "(a = b) AND (c > d)"


def describe_or() -> None:
    """Tests for Or class."""

    def it_renders_or_expression(render_context: vw.RenderContext) -> None:
        """Should render OR expression."""
        or_expr = Or(
            left=NotEquals(left=col("x"), right=col("y")),
            right=LessThan(left=col("m"), right=col("n")),
        )
        assert or_expr.__vw_render__(render_context) == "(x <> y) OR (m < n)"


def describe_not() -> None:
    """Tests for Not class."""

    def it_renders_not_expression(render_context: vw.RenderContext) -> None:
        """Should render NOT expression."""
        not_expr = Not(operand=Equals(left=col("active"), right=col("true")))
        assert not_expr.__vw_render__(render_context) == "NOT (active = true)"

    def it_creates_not_with_invert_operator(render_context: vw.RenderContext) -> None:
        """Should create Not expression with ~ operator."""
        result = ~(col("active") == col("true"))
        assert isinstance(result, Not)
        assert result.__vw_render__(render_context) == "NOT (active = true)"

    def it_negates_compound_expressions(render_context: vw.RenderContext) -> None:
        """Should negate compound AND/OR expressions."""
        expr = (col("a") == col("b")) & (col("c") > col("d"))
        negated = ~expr
        assert isinstance(negated, Not)
        assert negated.__vw_render__(render_context) == "NOT ((a = b) AND (c > d))"

    def it_combines_not_with_other_operators(render_context: vw.RenderContext) -> None:
        """Should combine NOT with AND/OR operators."""
        expr = ~(col("active") == col("true")) & (col("age") >= col("18"))
        assert expr.__vw_render__(render_context) == "(NOT (active = true)) AND (age >= 18)"


def describe_chained_expressions() -> None:
    """Tests for chained comparison expressions."""

    def it_renders_chained_comparisons(render_context: vw.RenderContext) -> None:
        """Should render chained comparison expressions."""
        expr = And(
            left=Equals(left=col("a"), right=col("b")),
            right=Or(
                left=LessThan(left=col("c"), right=col("d")),
                right=GreaterThanOrEqual(left=col("e"), right=col("f")),
            ),
        )
        assert expr.__vw_render__(render_context) == "(a = b) AND ((c < d) OR (e >= f))"
