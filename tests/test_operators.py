"""Tests for vw/operators.py module."""

import vw
from vw.column import col
from vw.operators import (
    Add,
    And,
    Between,
    Divide,
    Equals,
    GreaterThan,
    GreaterThanOrEqual,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    Not,
    NotBetween,
    NotEquals,
    NotLike,
    Or,
    Subtract,
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


def describe_is_null() -> None:
    """Tests for IsNull class."""

    def it_renders_is_null(render_context: vw.RenderContext) -> None:
        """Should render IS NULL expression."""
        is_null = IsNull(expr=col("deleted_at"))
        assert is_null.__vw_render__(render_context) == "deleted_at IS NULL"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create IsNull via .is_null() method."""
        result = col("deleted_at").is_null()
        assert isinstance(result, IsNull)
        assert result.__vw_render__(render_context) == "deleted_at IS NULL"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("users.deleted_at").is_null()
        assert result.__vw_render__(render_context) == "users.deleted_at IS NULL"


def describe_is_not_null() -> None:
    """Tests for IsNotNull class."""

    def it_renders_is_not_null(render_context: vw.RenderContext) -> None:
        """Should render IS NOT NULL expression."""
        is_not_null = IsNotNull(expr=col("name"))
        assert is_not_null.__vw_render__(render_context) == "name IS NOT NULL"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create IsNotNull via .is_not_null() method."""
        result = col("name").is_not_null()
        assert isinstance(result, IsNotNull)
        assert result.__vw_render__(render_context) == "name IS NOT NULL"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("users.email").is_not_null()
        assert result.__vw_render__(render_context) == "users.email IS NOT NULL"


def describe_null_with_logical_operators() -> None:
    """Tests for null checks combined with logical operators."""

    def it_combines_is_null_with_and(render_context: vw.RenderContext) -> None:
        """Should combine IS NULL with AND."""
        expr = col("deleted_at").is_null() & (col("status") == col("'active'"))
        assert expr.__vw_render__(render_context) == "(deleted_at IS NULL) AND (status = 'active')"

    def it_combines_is_not_null_with_or(render_context: vw.RenderContext) -> None:
        """Should combine IS NOT NULL with OR."""
        expr = col("email").is_not_null() | col("phone").is_not_null()
        assert expr.__vw_render__(render_context) == "(email IS NOT NULL) OR (phone IS NOT NULL)"

    def it_negates_is_null_with_not(render_context: vw.RenderContext) -> None:
        """Should negate IS NULL with NOT operator."""
        expr = ~col("deleted_at").is_null()
        assert expr.__vw_render__(render_context) == "NOT (deleted_at IS NULL)"


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


# -----------------------------------------------------------------------------
# Mathematical operators
# -----------------------------------------------------------------------------


def describe_add() -> None:
    """Tests for Add class."""

    def it_renders_addition(render_context: vw.RenderContext) -> None:
        """Should render addition with + operator."""
        add = Add(left=col("a"), right=col("b"))
        assert add.__vw_render__(render_context) == "a + b"

    def it_creates_via_dunder_method(render_context: vw.RenderContext) -> None:
        """Should create Add via + operator."""
        result = col("price") + col("tax")
        assert isinstance(result, Add)
        assert result.__vw_render__(render_context) == "price + tax"


def describe_subtract() -> None:
    """Tests for Subtract class."""

    def it_renders_subtraction(render_context: vw.RenderContext) -> None:
        """Should render subtraction with - operator."""
        subtract = Subtract(left=col("a"), right=col("b"))
        assert subtract.__vw_render__(render_context) == "a - b"

    def it_creates_via_dunder_method(render_context: vw.RenderContext) -> None:
        """Should create Subtract via - operator."""
        result = col("total") - col("discount")
        assert isinstance(result, Subtract)
        assert result.__vw_render__(render_context) == "total - discount"


def describe_multiply() -> None:
    """Tests for Multiply class."""

    def it_renders_multiplication(render_context: vw.RenderContext) -> None:
        """Should render multiplication with * operator."""
        multiply = Multiply(left=col("a"), right=col("b"))
        assert multiply.__vw_render__(render_context) == "a * b"

    def it_creates_via_dunder_method(render_context: vw.RenderContext) -> None:
        """Should create Multiply via * operator."""
        result = col("price") * col("quantity")
        assert isinstance(result, Multiply)
        assert result.__vw_render__(render_context) == "price * quantity"


def describe_divide() -> None:
    """Tests for Divide class."""

    def it_renders_division(render_context: vw.RenderContext) -> None:
        """Should render division with / operator."""
        divide = Divide(left=col("a"), right=col("b"))
        assert divide.__vw_render__(render_context) == "a / b"

    def it_creates_via_dunder_method(render_context: vw.RenderContext) -> None:
        """Should create Divide via / operator."""
        result = col("total") / col("count")
        assert isinstance(result, Divide)
        assert result.__vw_render__(render_context) == "total / count"


def describe_modulo() -> None:
    """Tests for Modulo class."""

    def it_renders_modulo(render_context: vw.RenderContext) -> None:
        """Should render modulo with % operator."""
        modulo = Modulo(left=col("a"), right=col("b"))
        assert modulo.__vw_render__(render_context) == "a % b"

    def it_creates_via_dunder_method(render_context: vw.RenderContext) -> None:
        """Should create Modulo via % operator."""
        result = col("value") % col("divisor")
        assert isinstance(result, Modulo)
        assert result.__vw_render__(render_context) == "value % divisor"


def describe_math_with_other_operators() -> None:
    """Tests for mathematical operators combined with other operators."""

    def it_combines_math_with_comparison(render_context: vw.RenderContext) -> None:
        """Should combine math with comparison operators."""
        expr = (col("price") * col("quantity")) > col("threshold")
        assert expr.__vw_render__(render_context) == "price * quantity > threshold"

    def it_chains_multiple_math_operators(render_context: vw.RenderContext) -> None:
        """Should chain multiple math operators."""
        expr = col("a") + col("b") - col("c")
        assert expr.__vw_render__(render_context) == "a + b - c"

    def it_combines_with_alias(render_context: vw.RenderContext) -> None:
        """Should work with alias."""
        expr = (col("price") * col("quantity")).alias("total")
        assert expr.__vw_render__(render_context) == "price * quantity AS total"


# -----------------------------------------------------------------------------
# LIKE operators
# -----------------------------------------------------------------------------


def describe_like() -> None:
    """Tests for Like class."""

    def it_renders_like_comparison(render_context: vw.RenderContext) -> None:
        """Should render LIKE comparison."""
        like = Like(left=col("name"), right="%john%")
        assert like.__vw_render__(render_context) == "name LIKE '%john%'"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create Like via .like() method."""
        result = col("name").like("%john%")
        assert isinstance(result, Like)
        assert result.__vw_render__(render_context) == "name LIKE '%john%'"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("users.name").like("%test%")
        assert result.__vw_render__(render_context) == "users.name LIKE '%test%'"


def describe_not_like() -> None:
    """Tests for NotLike class."""

    def it_renders_not_like_comparison(render_context: vw.RenderContext) -> None:
        """Should render NOT LIKE comparison."""
        not_like = NotLike(left=col("name"), right="%admin%")
        assert not_like.__vw_render__(render_context) == "name NOT LIKE '%admin%'"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create NotLike via .not_like() method."""
        result = col("name").not_like("%admin%")
        assert isinstance(result, NotLike)
        assert result.__vw_render__(render_context) == "name NOT LIKE '%admin%'"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("users.email").not_like("%spam%")
        assert result.__vw_render__(render_context) == "users.email NOT LIKE '%spam%'"


def describe_like_with_logical_operators() -> None:
    """Tests for LIKE combined with logical operators."""

    def it_combines_like_with_and(render_context: vw.RenderContext) -> None:
        """Should combine LIKE with AND."""
        expr = col("name").like("%john%") & (col("active") == col("true"))
        assert expr.__vw_render__(render_context) == "(name LIKE '%john%') AND (active = true)"

    def it_combines_not_like_with_or(render_context: vw.RenderContext) -> None:
        """Should combine NOT LIKE with OR."""
        expr = col("name").not_like("%admin%") | col("name").not_like("%system%")
        assert expr.__vw_render__(render_context) == "(name NOT LIKE '%admin%') OR (name NOT LIKE '%system%')"


# -----------------------------------------------------------------------------
# BETWEEN operators
# -----------------------------------------------------------------------------


def describe_between() -> None:
    """Tests for Between class."""

    def it_renders_between_comparison(render_context: vw.RenderContext) -> None:
        """Should render BETWEEN comparison."""
        between = Between(expr=col("age"), lower_bound=col("18"), upper_bound=col("65"))
        assert between.__vw_render__(render_context) == "age BETWEEN 18 AND 65"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create Between via .between() method."""
        result = col("price").between(col("10"), col("100"))
        assert isinstance(result, Between)
        assert result.__vw_render__(render_context) == "price BETWEEN 10 AND 100"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("users.age").between(col("18"), col("65"))
        assert result.__vw_render__(render_context) == "users.age BETWEEN 18 AND 65"

    def it_works_with_expressions_as_bounds(render_context: vw.RenderContext) -> None:
        """Should work with expressions as bounds."""
        result = col("score").between(col("min_score"), col("max_score"))
        assert result.__vw_render__(render_context) == "score BETWEEN min_score AND max_score"

    def it_works_with_math_expressions(render_context: vw.RenderContext) -> None:
        """Should work with mathematical expressions."""
        result = col("age").between(col("18") + col("0"), col("65") - col("0"))
        assert result.__vw_render__(render_context) == "age BETWEEN 18 + 0 AND 65 - 0"


def describe_not_between() -> None:
    """Tests for NotBetween class."""

    def it_renders_not_between_comparison(render_context: vw.RenderContext) -> None:
        """Should render NOT BETWEEN comparison."""
        not_between = NotBetween(expr=col("score"), lower_bound=col("0"), upper_bound=col("100"))
        assert not_between.__vw_render__(render_context) == "score NOT BETWEEN 0 AND 100"

    def it_creates_via_method(render_context: vw.RenderContext) -> None:
        """Should create NotBetween via .not_between() method."""
        result = col("age").not_between(col("0"), col("18"))
        assert isinstance(result, NotBetween)
        assert result.__vw_render__(render_context) == "age NOT BETWEEN 0 AND 18"

    def it_works_with_qualified_column(render_context: vw.RenderContext) -> None:
        """Should work with qualified column names."""
        result = col("products.price").not_between(col("1"), col("10"))
        assert result.__vw_render__(render_context) == "products.price NOT BETWEEN 1 AND 10"

    def it_works_with_expressions_as_bounds(render_context: vw.RenderContext) -> None:
        """Should work with expressions as bounds."""
        result = col("value").not_between(col("lower"), col("upper"))
        assert result.__vw_render__(render_context) == "value NOT BETWEEN lower AND upper"


def describe_between_with_logical_operators() -> None:
    """Tests for BETWEEN combined with logical operators."""

    def it_combines_between_with_and(render_context: vw.RenderContext) -> None:
        """Should combine BETWEEN with AND."""
        expr = col("age").between(col("18"), col("65")) & (col("status") == col("'active'"))
        assert expr.__vw_render__(render_context) == "(age BETWEEN 18 AND 65) AND (status = 'active')"

    def it_combines_not_between_with_or(render_context: vw.RenderContext) -> None:
        """Should combine NOT BETWEEN with OR."""
        expr = col("score").not_between(col("0"), col("60")) | col("grade").not_between(col("90"), col("100"))
        assert expr.__vw_render__(render_context) == "(score NOT BETWEEN 0 AND 60) OR (grade NOT BETWEEN 90 AND 100)"

    def it_negates_between_with_not(render_context: vw.RenderContext) -> None:
        """Should negate BETWEEN with NOT operator."""
        expr = ~col("age").between(col("18"), col("65"))
        assert expr.__vw_render__(render_context) == "NOT (age BETWEEN 18 AND 65)"

    def it_combines_multiple_between_clauses(render_context: vw.RenderContext) -> None:
        """Should combine multiple BETWEEN clauses."""
        expr = (
            col("age").between(col("18"), col("65"))
            & col("salary").between(col("30000"), col("100000"))
            & col("experience").not_between(col("0"), col("1"))
        )
        assert (
            expr.__vw_render__(render_context)
            == "((age BETWEEN 18 AND 65) AND (salary BETWEEN 30000 AND 100000)) AND (experience NOT BETWEEN 0 AND 1)"
        )

    def it_works_with_mathematical_expressions(render_context: vw.RenderContext) -> None:
        """Should work with mathematical expressions."""
        expr = (col("price") * col("quantity")).between(col("100"), col("1000"))
        assert expr.__vw_render__(render_context) == "price * quantity BETWEEN 100 AND 1000"

    def it_works_with_complex_nested_expressions(render_context: vw.RenderContext) -> None:
        """Should work with complex nested expressions."""
        expr = (col("base_salary") + col("bonus")).between(col("50000"), col("200000")) & col(
            "years_experience"
        ).between(col("2"), col("10"))
        assert (
            expr.__vw_render__(render_context)
            == "(base_salary + bonus BETWEEN 50000 AND 200000) AND (years_experience BETWEEN 2 AND 10)"
        )
