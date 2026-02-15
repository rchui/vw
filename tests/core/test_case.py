"""Tests for core CASE/WHEN expression builders."""

from vw.core.case import CaseExpression, When
from vw.core.states import Case, Column, Literal, WhenThen
from vw.postgres import col, lit, render, when
from vw.postgres.base import Expression


def describe_when_builder() -> None:
    """Test When builder class."""

    def it_creates_when_with_condition() -> None:
        """When builder should store condition and prior_whens."""
        condition = col("age") >= lit(18)
        builder = when(condition)

        assert isinstance(builder, When)
        assert builder.condition == condition.state
        assert builder.prior_whens == ()

    def it_stores_prior_whens_from_case_expression() -> None:
        """When builder created from CaseExpression should store prior whens."""
        expr = when(col("x") == lit(1)).then(lit("a")).when(col("y") == lit(2))

        assert isinstance(expr, When)
        assert len(expr.prior_whens) == 1
        assert isinstance(expr.prior_whens[0], WhenThen)

    def it_converts_condition_to_state() -> None:
        """When builder should extract state from Expression."""
        condition = col("status") == lit("active")
        builder = when(condition)

        assert isinstance(builder.condition, Column) or hasattr(builder.condition, "left")
        assert builder.condition == condition.state


def describe_when_then() -> None:
    """Test .then() method on When builder."""

    def it_returns_case_expression() -> None:
        """then() should return a CaseExpression."""
        result = when(col("x") == lit(1)).then(lit("a"))

        assert isinstance(result, CaseExpression)

    def it_creates_when_then_state() -> None:
        """then() should create WhenThen state with condition and result."""
        builder = when(col("x") == lit(1))
        expr = builder.then(lit("a"))

        assert len(expr.whens) == 1
        assert isinstance(expr.whens[0], WhenThen)
        assert expr.whens[0].condition == builder.condition
        # Check structural equality - dataclasses with eq=False use identity
        result_state = lit("a").state
        assert isinstance(expr.whens[0].result, Literal)
        assert isinstance(result_state, Literal)
        assert expr.whens[0].result.value == result_state.value

    def it_accumulates_prior_whens() -> None:
        """then() should accumulate prior WhenThen pairs."""
        expr = when(col("x") == lit(1)).then(lit("a")).when(col("y") == lit(2)).then(lit("b"))

        assert isinstance(expr, CaseExpression)
        assert len(expr.whens) == 2
        assert isinstance(expr.whens[0], WhenThen)
        assert isinstance(expr.whens[1], WhenThen)


def describe_case_expression() -> None:
    """Test CaseExpression builder class."""

    def it_creates_with_whens_tuple() -> None:
        """CaseExpression should store tuple of WhenThen states."""
        expr = when(col("x") == lit(1)).then(lit("a"))

        assert isinstance(expr, CaseExpression)
        assert isinstance(expr.whens, tuple)
        assert len(expr.whens) == 1

    def it_supports_when_chaining() -> None:
        """when() on CaseExpression should return When builder."""
        case_expr = when(col("x") == lit(1)).then(lit("a"))
        next_when = case_expr.when(col("y") == lit(2))

        assert isinstance(next_when, When)
        assert next_when.prior_whens == case_expr.whens


def describe_case_expression_otherwise() -> None:
    """Test .otherwise() method on CaseExpression."""

    def it_returns_expression() -> None:
        """otherwise() should return an Expression."""
        expr = when(col("x") == lit(1)).then(lit("a")).otherwise(lit("b"))

        assert isinstance(expr, Expression)

    def it_creates_case_state_with_else() -> None:
        """otherwise() should create Case state with else_result."""
        expr = when(col("x") == lit(1)).then(lit("a")).otherwise(lit("b"))

        assert isinstance(expr.state, Case)
        assert len(expr.state.whens) == 1
        # Check structural equality
        assert isinstance(expr.state.else_result, Literal)
        assert expr.state.else_result.value == "b"

    def it_preserves_multiple_whens() -> None:
        """otherwise() should preserve all WhenThen pairs."""
        expr = when(col("x") == lit(1)).then(lit("a")).when(col("y") == lit(2)).then(lit("b")).otherwise(lit("c"))

        assert isinstance(expr.state, Case)
        assert len(expr.state.whens) == 2
        # Check structural equality
        assert isinstance(expr.state.else_result, Literal)
        assert expr.state.else_result.value == "c"


def describe_case_expression_end() -> None:
    """Test .end() method on CaseExpression."""

    def it_returns_expression() -> None:
        """end() should return an Expression."""
        expr = when(col("x") == lit(1)).then(lit("a")).end()

        assert isinstance(expr, Expression)

    def it_creates_case_state_without_else() -> None:
        """end() should create Case state with no else_result."""
        expr = when(col("x") == lit(1)).then(lit("a")).end()

        assert isinstance(expr.state, Case)
        assert len(expr.state.whens) == 1
        assert expr.state.else_result is None

    def it_preserves_multiple_whens() -> None:
        """end() should preserve all WhenThen pairs."""
        expr = when(col("x") == lit(1)).then(lit("a")).when(col("y") == lit(2)).then(lit("b")).end()

        assert isinstance(expr.state, Case)
        assert len(expr.state.whens) == 2
        assert expr.state.else_result is None


def describe_single_when_then() -> None:
    """Test single WHEN/THEN expressions."""

    def it_renders_with_otherwise() -> None:
        """Single when().then().otherwise() should render correctly."""
        expr = when(col("status") == lit("active")).then(lit(1)).otherwise(lit(0))
        query = render(expr)

        assert query.query == "CASE WHEN status = 'active' THEN 1 ELSE 0 END"
        assert query.params == {}

    def it_renders_with_end() -> None:
        """Single when().then().end() should render without ELSE."""
        expr = when(col("status") == lit("active")).then(lit(1)).end()
        query = render(expr)

        assert query.query == "CASE WHEN status = 'active' THEN 1 END"
        assert query.params == {}


def describe_multiple_when_then() -> None:
    """Test multiple WHEN/THEN expressions."""

    def it_renders_two_whens_with_otherwise() -> None:
        """Two when clauses with otherwise should render correctly."""
        expr = (
            when(col("age") >= lit(18))
            .then(lit("adult"))
            .when(col("age") >= lit(13))
            .then(lit("teen"))
            .otherwise(lit("child"))
        )
        query = render(expr)

        assert query.query == "CASE WHEN age >= 18 THEN 'adult' WHEN age >= 13 THEN 'teen' ELSE 'child' END"
        assert query.params == {}

    def it_renders_three_whens_with_otherwise() -> None:
        """Three when clauses should render correctly."""
        expr = (
            when(col("score") >= lit(90))
            .then(lit("A"))
            .when(col("score") >= lit(80))
            .then(lit("B"))
            .when(col("score") >= lit(70))
            .then(lit("C"))
            .otherwise(lit("F"))
        )
        query = render(expr)

        assert (
            query.query
            == "CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END"
        )
        assert query.params == {}

    def it_renders_two_whens_with_end() -> None:
        """Two when clauses with end() should render without ELSE."""
        expr = when(col("x") == lit(1)).then(lit("a")).when(col("y") == lit(2)).then(lit("b")).end()
        query = render(expr)

        assert query.query == "CASE WHEN x = 1 THEN 'a' WHEN y = 2 THEN 'b' END"
        assert query.params == {}


def describe_case_with_alias() -> None:
    """Test CASE expressions with aliases."""

    def it_renders_with_alias() -> None:
        """CASE expression should support .alias()."""
        expr = when(col("status") == lit("active")).then(lit(1)).otherwise(lit(0)).alias("is_active")
        query = render(expr)

        assert query.query == "CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active"
        assert query.params == {}


def describe_case_with_complex_conditions() -> None:
    """Test CASE expressions with complex conditions."""

    def it_renders_with_and_condition() -> None:
        """CASE with AND condition should render correctly."""
        expr = when((col("age") >= lit(18)) & (col("verified") == lit(True))).then(lit("ok")).otherwise(lit("pending"))
        query = render(expr)

        assert query.query == "CASE WHEN (age >= 18) AND (verified = TRUE) THEN 'ok' ELSE 'pending' END"
        assert query.params == {}

    def it_renders_with_or_condition() -> None:
        """CASE with OR condition should render correctly."""
        expr = (
            when((col("admin") == lit(True)) | (col("owner") == lit(True)))
            .then(lit("allowed"))
            .otherwise(lit("denied"))
        )
        query = render(expr)

        assert query.query == "CASE WHEN (admin = TRUE) OR (owner = TRUE) THEN 'allowed' ELSE 'denied' END"
        assert query.params == {}


def describe_case_with_complex_results() -> None:
    """Test CASE expressions with complex results."""

    def it_renders_with_arithmetic_result() -> None:
        """CASE with arithmetic expression as result."""
        expr = when(col("premium") == lit(True)).then(col("price") * lit(0.9)).otherwise(col("price"))
        query = render(expr)

        assert query.query == "CASE WHEN premium = TRUE THEN price * 0.9 ELSE price END"
        assert query.params == {}

    def it_renders_with_column_result() -> None:
        """CASE with column references as results."""
        expr = when(col("use_alt") == lit(True)).then(col("alt_name")).otherwise(col("name"))
        query = render(expr)

        assert query.query == "CASE WHEN use_alt = TRUE THEN alt_name ELSE name END"
        assert query.params == {}


def describe_nested_case() -> None:
    """Test nested CASE expressions."""

    def it_renders_nested_case_in_result() -> None:
        """CASE with nested CASE as result should render correctly."""
        inner = when(col("score") >= lit(90)).then(lit("gold")).otherwise(lit("silver"))
        outer = when(col("tier") == lit("premium")).then(inner).otherwise(lit("bronze"))
        query = render(outer)

        assert (
            query.query
            == "CASE WHEN tier = 'premium' THEN CASE WHEN score >= 90 THEN 'gold' ELSE 'silver' END ELSE 'bronze' END"
        )
        assert query.params == {}


def describe_when_then_state() -> None:
    """Test WhenThen state dataclass."""

    def it_creates_with_condition_and_result() -> None:
        """WhenThen should store condition and result expressions."""
        condition_state = Column(name="x")
        result_state = Literal(value="a")
        when_then = WhenThen(condition=condition_state, result=result_state)

        assert when_then.condition == condition_state
        assert when_then.result == result_state

    def it_is_frozen() -> None:
        """WhenThen should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        when_then = WhenThen(condition=Column(name="x"), result=Literal(value="a"))

        with pytest.raises(dataclasses.FrozenInstanceError):
            when_then.condition = Column(name="y")  # type: ignore

    def it_is_not_equal_by_default() -> None:
        """WhenThen should use identity equality (eq=False)."""
        when_then1 = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        when_then2 = WhenThen(condition=Column(name="x"), result=Literal(value="a"))

        assert when_then1 is not when_then2
        # eq=False means identity comparison, not structural
        assert (when_then1 == when_then2) == (when_then1 is when_then2)


def describe_case_state() -> None:
    """Test Case state dataclass."""

    def it_creates_with_whens_and_else() -> None:
        """Case should store whens tuple and optional else_result."""
        when_then = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        case_state = Case(whens=(when_then,), else_result=Literal(value="b"))

        assert len(case_state.whens) == 1
        assert case_state.whens[0] == when_then
        # Check structural equality
        assert isinstance(case_state.else_result, Literal)
        assert case_state.else_result.value == "b"

    def it_creates_without_else() -> None:
        """Case should allow None for else_result."""
        when_then = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        case_state = Case(whens=(when_then,))

        assert len(case_state.whens) == 1
        assert case_state.else_result is None

    def it_creates_with_multiple_whens() -> None:
        """Case should store multiple WhenThen pairs."""
        when1 = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        when2 = WhenThen(condition=Column(name="y"), result=Literal(value="b"))
        case_state = Case(whens=(when1, when2), else_result=Literal(value="c"))

        assert len(case_state.whens) == 2
        assert case_state.whens[0] == when1
        assert case_state.whens[1] == when2

    def it_is_frozen() -> None:
        """Case should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        when_then = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        case_state = Case(whens=(when_then,))

        with pytest.raises(dataclasses.FrozenInstanceError):
            case_state.whens = ()  # type: ignore

    def it_is_expr_subclass() -> None:
        """Case should inherit from Expr base class."""
        from vw.core.states import Expr

        when_then = WhenThen(condition=Column(name="x"), result=Literal(value="a"))
        case_state = Case(whens=(when_then,))

        assert isinstance(case_state, Expr)
