"""Tests for core pattern matching expression builders."""

from vw.core.states import Between, Column, IsIn, IsNotIn, Like, NotBetween, NotLike
from vw.postgres import col, lit, param, render
from vw.postgres.base import Expression


def describe_like() -> None:
    """Test .like() method and Like state."""

    def it_creates_like_state() -> None:
        """like() should create Like state with left and right expressions."""
        expr = col("name").like(lit("A%"))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, Like)
        assert isinstance(expr.state.left, Column)
        assert expr.state.left.name == "name"

    def it_accepts_literal_pattern() -> None:
        """like() should accept literal pattern."""
        expr = col("email").like(lit("%@example.com"))

        assert isinstance(expr.state, Like)
        # Check that right expression exists
        assert expr.state.right is not None

    def it_accepts_parameter_pattern() -> None:
        """like() should accept parameter pattern."""
        expr = col("name").like(param("pattern", "test%"))

        assert isinstance(expr.state, Like)
        assert expr.state.right is not None

    def it_accepts_column_pattern() -> None:
        """like() should accept column reference as pattern."""
        expr = col("name").like(col("pattern_column"))

        assert isinstance(expr.state, Like)
        assert expr.state.right is not None

    def it_renders_with_literal() -> None:
        """like() with literal should render correctly."""
        expr = col("name").like(lit("A%"))
        result = render(expr)

        assert result.query == "name LIKE 'A%'"
        assert result.params == {}

    def it_renders_with_parameter() -> None:
        """like() with parameter should render correctly."""
        expr = col("email").like(param("pattern", "%@example.com"))
        result = render(expr)

        assert result.query == "email LIKE $pattern"
        assert result.params == {"pattern": "%@example.com"}

    def it_renders_with_column() -> None:
        """like() with column reference should render correctly."""
        expr = col("name").like(col("search_pattern"))
        result = render(expr)

        assert result.query == "name LIKE search_pattern"
        assert result.params == {}


def describe_not_like() -> None:
    """Test .not_like() method and NotLike state."""

    def it_creates_not_like_state() -> None:
        """not_like() should create NotLike state with left and right expressions."""
        expr = col("name").not_like(lit("B%"))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, NotLike)
        assert isinstance(expr.state.left, Column)
        assert expr.state.left.name == "name"

    def it_accepts_literal_pattern() -> None:
        """not_like() should accept literal pattern."""
        expr = col("email").not_like(lit("%spam.com"))

        assert isinstance(expr.state, NotLike)
        assert expr.state.right is not None

    def it_accepts_parameter_pattern() -> None:
        """not_like() should accept parameter pattern."""
        expr = col("name").not_like(param("pattern", "ignore%"))

        assert isinstance(expr.state, NotLike)
        assert expr.state.right is not None

    def it_accepts_column_pattern() -> None:
        """not_like() should accept column reference as pattern."""
        expr = col("name").not_like(col("exclude_pattern"))

        assert isinstance(expr.state, NotLike)
        assert expr.state.right is not None

    def it_renders_with_literal() -> None:
        """not_like() with literal should render correctly."""
        expr = col("name").not_like(lit("test%"))
        result = render(expr)

        assert result.query == "name NOT LIKE 'test%'"
        assert result.params == {}

    def it_renders_with_parameter() -> None:
        """not_like() with parameter should render correctly."""
        expr = col("name").not_like(param("pattern", "ignore%"))
        result = render(expr)

        assert result.query == "name NOT LIKE $pattern"
        assert result.params == {"pattern": "ignore%"}

    def it_renders_with_column() -> None:
        """not_like() with column reference should render correctly."""
        expr = col("name").not_like(col("exclude_pattern"))
        result = render(expr)

        assert result.query == "name NOT LIKE exclude_pattern"
        assert result.params == {}


def describe_is_in() -> None:
    """Test .is_in() method and IsIn state."""

    def it_creates_is_in_state() -> None:
        """is_in() should create IsIn state with expr and values tuple."""
        expr = col("status").is_in(lit("active"), lit("pending"))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, IsIn)
        assert isinstance(expr.state.expr, Column)
        assert expr.state.expr.name == "status"
        assert isinstance(expr.state.values, tuple)
        assert len(expr.state.values) == 2

    def it_accepts_single_value() -> None:
        """is_in() should accept single value."""
        expr = col("id").is_in(lit(1))

        assert isinstance(expr.state, IsIn)
        assert len(expr.state.values) == 1

    def it_accepts_multiple_values() -> None:
        """is_in() should accept multiple values."""
        expr = col("status").is_in(lit("a"), lit("b"), lit("c"), lit("d"))

        assert isinstance(expr.state, IsIn)
        assert len(expr.state.values) == 4

    def it_accepts_literal_values() -> None:
        """is_in() should accept literal values."""
        expr = col("id").is_in(lit(1), lit(2), lit(3))

        assert isinstance(expr.state, IsIn)
        assert len(expr.state.values) == 3

    def it_accepts_parameter_values() -> None:
        """is_in() should accept parameter values."""
        expr = col("status").is_in(param("s1", "active"), param("s2", "pending"))

        assert isinstance(expr.state, IsIn)
        assert len(expr.state.values) == 2

    def it_accepts_mixed_values() -> None:
        """is_in() should accept mixed expression types."""
        expr = col("value").is_in(lit(1), param("p", 2), col("other"))

        assert isinstance(expr.state, IsIn)
        assert len(expr.state.values) == 3

    def it_renders_with_literals() -> None:
        """is_in() with literals should render correctly."""
        expr = col("status").is_in(lit("active"), lit("pending"), lit("verified"))
        result = render(expr)

        assert result.query == "status IN ('active', 'pending', 'verified')"
        assert result.params == {}

    def it_renders_with_parameters() -> None:
        """is_in() with parameters should render correctly."""
        expr = col("status").is_in(param("s1", "active"), param("s2", "pending"))
        result = render(expr)

        assert result.query == "status IN ($s1, $s2)"
        assert result.params == {"s1": "active", "s2": "pending"}

    def it_renders_single_value() -> None:
        """is_in() with single value should render correctly."""
        expr = col("id").is_in(lit(42))
        result = render(expr)

        assert result.query == "id IN (42)"
        assert result.params == {}


def describe_is_not_in() -> None:
    """Test .is_not_in() method and IsNotIn state."""

    def it_creates_is_not_in_state() -> None:
        """is_not_in() should create IsNotIn state with expr and values tuple."""
        expr = col("role").is_not_in(lit("banned"), lit("suspended"))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, IsNotIn)
        assert isinstance(expr.state.expr, Column)
        assert expr.state.expr.name == "role"
        assert isinstance(expr.state.values, tuple)
        assert len(expr.state.values) == 2

    def it_accepts_single_value() -> None:
        """is_not_in() should accept single value."""
        expr = col("id").is_not_in(lit(0))

        assert isinstance(expr.state, IsNotIn)
        assert len(expr.state.values) == 1

    def it_accepts_multiple_values() -> None:
        """is_not_in() should accept multiple values."""
        expr = col("status").is_not_in(lit("deleted"), lit("archived"), lit("hidden"))

        assert isinstance(expr.state, IsNotIn)
        assert len(expr.state.values) == 3

    def it_accepts_literal_values() -> None:
        """is_not_in() should accept literal values."""
        expr = col("id").is_not_in(lit(1), lit(2))

        assert isinstance(expr.state, IsNotIn)
        assert len(expr.state.values) == 2

    def it_accepts_parameter_values() -> None:
        """is_not_in() should accept parameter values."""
        expr = col("role").is_not_in(param("r1", "banned"), param("r2", "suspended"))

        assert isinstance(expr.state, IsNotIn)
        assert len(expr.state.values) == 2

    def it_accepts_mixed_values() -> None:
        """is_not_in() should accept mixed expression types."""
        expr = col("value").is_not_in(lit(0), param("p", -1), col("invalid_value"))

        assert isinstance(expr.state, IsNotIn)
        assert len(expr.state.values) == 3

    def it_renders_with_literals() -> None:
        """is_not_in() with literals should render correctly."""
        expr = col("status").is_not_in(lit("deleted"), lit("archived"))
        result = render(expr)

        assert result.query == "status NOT IN ('deleted', 'archived')"
        assert result.params == {}

    def it_renders_with_parameters() -> None:
        """is_not_in() with parameters should render correctly."""
        expr = col("role").is_not_in(param("r1", "banned"), param("r2", "suspended"))
        result = render(expr)

        assert result.query == "role NOT IN ($r1, $r2)"
        assert result.params == {"r1": "banned", "r2": "suspended"}

    def it_renders_single_value() -> None:
        """is_not_in() with single value should render correctly."""
        expr = col("id").is_not_in(lit(0))
        result = render(expr)

        assert result.query == "id NOT IN (0)"
        assert result.params == {}


def describe_between() -> None:
    """Test .between() method and Between state."""

    def it_creates_between_state() -> None:
        """between() should create Between state with expr, lower_bound, and upper_bound."""
        expr = col("age").between(lit(18), lit(65))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, Between)
        assert isinstance(expr.state.expr, Column)
        assert expr.state.expr.name == "age"
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_literal_bounds() -> None:
        """between() should accept literal bounds."""
        expr = col("price").between(lit(10), lit(100))

        assert isinstance(expr.state, Between)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_parameter_bounds() -> None:
        """between() should accept parameter bounds."""
        expr = col("price").between(param("low", 10), param("high", 100))

        assert isinstance(expr.state, Between)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_column_bounds() -> None:
        """between() should accept column reference bounds."""
        expr = col("value").between(col("min_value"), col("max_value"))

        assert isinstance(expr.state, Between)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_mixed_bounds() -> None:
        """between() should accept mixed expression types."""
        expr = col("value").between(lit(0), param("max", 100))

        assert isinstance(expr.state, Between)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_renders_with_literals() -> None:
        """between() with literals should render correctly."""
        expr = col("age").between(lit(18), lit(65))
        result = render(expr)

        assert result.query == "age BETWEEN 18 AND 65"
        assert result.params == {}

    def it_renders_with_parameters() -> None:
        """between() with parameters should render correctly."""
        expr = col("price").between(param("low", 10), param("high", 100))
        result = render(expr)

        assert result.query == "price BETWEEN $low AND $high"
        assert result.params == {"low": 10, "high": 100}

    def it_renders_with_columns() -> None:
        """between() with column references should render correctly."""
        expr = col("value").between(col("min_val"), col("max_val"))
        result = render(expr)

        assert result.query == "value BETWEEN min_val AND max_val"
        assert result.params == {}

    def it_renders_with_mixed_expressions() -> None:
        """between() with mixed expression types should render correctly."""
        expr = col("score").between(lit(0), param("max", 100))
        result = render(expr)

        assert result.query == "score BETWEEN 0 AND $max"
        assert result.params == {"max": 100}


def describe_not_between() -> None:
    """Test .not_between() method and NotBetween state."""

    def it_creates_not_between_state() -> None:
        """not_between() should create NotBetween state with expr, lower_bound, and upper_bound."""
        expr = col("age").not_between(lit(0), lit(17))

        assert isinstance(expr, Expression)
        assert isinstance(expr.state, NotBetween)
        assert isinstance(expr.state.expr, Column)
        assert expr.state.expr.name == "age"
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_literal_bounds() -> None:
        """not_between() should accept literal bounds."""
        expr = col("price").not_between(lit(50), lit(200))

        assert isinstance(expr.state, NotBetween)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_parameter_bounds() -> None:
        """not_between() should accept parameter bounds."""
        expr = col("price").not_between(param("low", 50), param("high", 200))

        assert isinstance(expr.state, NotBetween)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_column_bounds() -> None:
        """not_between() should accept column reference bounds."""
        expr = col("value").not_between(col("exclude_min"), col("exclude_max"))

        assert isinstance(expr.state, NotBetween)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_accepts_mixed_bounds() -> None:
        """not_between() should accept mixed expression types."""
        expr = col("value").not_between(param("low", -10), col("threshold"))

        assert isinstance(expr.state, NotBetween)
        assert expr.state.lower_bound is not None
        assert expr.state.upper_bound is not None

    def it_renders_with_literals() -> None:
        """not_between() with literals should render correctly."""
        expr = col("age").not_between(lit(0), lit(17))
        result = render(expr)

        assert result.query == "age NOT BETWEEN 0 AND 17"
        assert result.params == {}

    def it_renders_with_parameters() -> None:
        """not_between() with parameters should render correctly."""
        expr = col("price").not_between(param("low", 50), param("high", 200))
        result = render(expr)

        assert result.query == "price NOT BETWEEN $low AND $high"
        assert result.params == {"low": 50, "high": 200}

    def it_renders_with_columns() -> None:
        """not_between() with column references should render correctly."""
        expr = col("value").not_between(col("min"), col("max"))
        result = render(expr)

        assert result.query == "value NOT BETWEEN min AND max"
        assert result.params == {}

    def it_renders_with_mixed_expressions() -> None:
        """not_between() with mixed expression types should render correctly."""
        expr = col("temp").not_between(param("freezing", 0), lit(100))
        result = render(expr)

        assert result.query == "temp NOT BETWEEN $freezing AND 100"
        assert result.params == {"freezing": 0}


def describe_like_state() -> None:
    """Test Like state dataclass."""

    def it_creates_with_left_and_right() -> None:
        """Like should store left and right expressions."""
        left = Column(name="name")
        right = Column(name="pattern")
        like_state = Like(left=left, right=right)

        assert like_state.left == left
        assert like_state.right == right

    def it_is_frozen() -> None:
        """Like should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        like_state = Like(left=Column(name="a"), right=Column(name="b"))

        with pytest.raises(dataclasses.FrozenInstanceError):
            like_state.left = Column(name="c")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        like1 = Like(left=Column(name="a"), right=Column(name="b"))
        like2 = Like(left=Column(name="a"), right=Column(name="b"))

        assert like1 is not like2
        assert like1 == like2

    def it_is_expr_subclass() -> None:
        """Like should inherit from Expr base class."""
        from vw.core.states import Expr

        like_state = Like(left=Column(name="a"), right=Column(name="b"))

        assert isinstance(like_state, Expr)


def describe_not_like_state() -> None:
    """Test NotLike state dataclass."""

    def it_creates_with_left_and_right() -> None:
        """NotLike should store left and right expressions."""
        left = Column(name="name")
        right = Column(name="pattern")
        not_like_state = NotLike(left=left, right=right)

        assert not_like_state.left == left
        assert not_like_state.right == right

    def it_is_frozen() -> None:
        """NotLike should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        not_like_state = NotLike(left=Column(name="a"), right=Column(name="b"))

        with pytest.raises(dataclasses.FrozenInstanceError):
            not_like_state.left = Column(name="c")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        not_like1 = NotLike(left=Column(name="a"), right=Column(name="b"))
        not_like2 = NotLike(left=Column(name="a"), right=Column(name="b"))

        assert not_like1 is not not_like2
        assert not_like1 == not_like2

    def it_is_expr_subclass() -> None:
        """NotLike should inherit from Expr base class."""
        from vw.core.states import Expr

        not_like_state = NotLike(left=Column(name="a"), right=Column(name="b"))

        assert isinstance(not_like_state, Expr)


def describe_is_in_state() -> None:
    """Test IsIn state dataclass."""

    def it_creates_with_expr_and_values() -> None:
        """IsIn should store expr and values tuple."""
        expr = Column(name="status")
        values = (Column(name="'active'"), Column(name="'pending'"))
        is_in_state = IsIn(expr=expr, values=values)

        assert is_in_state.expr == expr
        assert is_in_state.values == values
        assert len(is_in_state.values) == 2

    def it_accepts_single_value() -> None:
        """IsIn should accept single value tuple."""
        expr = Column(name="id")
        values = (Column(name="1"),)
        is_in_state = IsIn(expr=expr, values=values)

        assert len(is_in_state.values) == 1

    def it_accepts_multiple_values() -> None:
        """IsIn should accept multiple values tuple."""
        expr = Column(name="id")
        values = (Column(name="1"), Column(name="2"), Column(name="3"), Column(name="4"))
        is_in_state = IsIn(expr=expr, values=values)

        assert len(is_in_state.values) == 4

    def it_is_frozen() -> None:
        """IsIn should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        is_in_state = IsIn(expr=Column(name="id"), values=(Column(name="1"),))

        with pytest.raises(dataclasses.FrozenInstanceError):
            is_in_state.expr = Column(name="other")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        is_in1 = IsIn(expr=Column(name="id"), values=(Column(name="1"),))
        is_in2 = IsIn(expr=Column(name="id"), values=(Column(name="1"),))

        assert is_in1 is not is_in2
        assert is_in1 == is_in2

    def it_is_expr_subclass() -> None:
        """IsIn should inherit from Expr base class."""
        from vw.core.states import Expr

        is_in_state = IsIn(expr=Column(name="id"), values=(Column(name="1"),))

        assert isinstance(is_in_state, Expr)


def describe_is_not_in_state() -> None:
    """Test IsNotIn state dataclass."""

    def it_creates_with_expr_and_values() -> None:
        """IsNotIn should store expr and values tuple."""
        expr = Column(name="role")
        values = (Column(name="'banned'"), Column(name="'suspended'"))
        is_not_in_state = IsNotIn(expr=expr, values=values)

        assert is_not_in_state.expr == expr
        assert is_not_in_state.values == values
        assert len(is_not_in_state.values) == 2

    def it_accepts_single_value() -> None:
        """IsNotIn should accept single value tuple."""
        expr = Column(name="id")
        values = (Column(name="0"),)
        is_not_in_state = IsNotIn(expr=expr, values=values)

        assert len(is_not_in_state.values) == 1

    def it_accepts_multiple_values() -> None:
        """IsNotIn should accept multiple values tuple."""
        expr = Column(name="status")
        values = (Column(name="'a'"), Column(name="'b'"), Column(name="'c'"))
        is_not_in_state = IsNotIn(expr=expr, values=values)

        assert len(is_not_in_state.values) == 3

    def it_is_frozen() -> None:
        """IsNotIn should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        is_not_in_state = IsNotIn(expr=Column(name="id"), values=(Column(name="0"),))

        with pytest.raises(dataclasses.FrozenInstanceError):
            is_not_in_state.expr = Column(name="other")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        is_not_in1 = IsNotIn(expr=Column(name="id"), values=(Column(name="0"),))
        is_not_in2 = IsNotIn(expr=Column(name="id"), values=(Column(name="0"),))

        assert is_not_in1 is not is_not_in2
        assert is_not_in1 == is_not_in2

    def it_is_expr_subclass() -> None:
        """IsNotIn should inherit from Expr base class."""
        from vw.core.states import Expr

        is_not_in_state = IsNotIn(expr=Column(name="id"), values=(Column(name="0"),))

        assert isinstance(is_not_in_state, Expr)


def describe_between_state() -> None:
    """Test Between state dataclass."""

    def it_creates_with_expr_and_bounds() -> None:
        """Between should store expr, lower_bound, and upper_bound."""
        expr = Column(name="age")
        lower = Column(name="18")
        upper = Column(name="65")
        between_state = Between(expr=expr, lower_bound=lower, upper_bound=upper)

        assert between_state.expr == expr
        assert between_state.lower_bound == lower
        assert between_state.upper_bound == upper

    def it_is_frozen() -> None:
        """Between should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        between_state = Between(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))

        with pytest.raises(dataclasses.FrozenInstanceError):
            between_state.expr = Column(name="other")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        between1 = Between(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))
        between2 = Between(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))

        assert between1 is not between2
        assert between1 == between2

    def it_is_expr_subclass() -> None:
        """Between should inherit from Expr base class."""
        from vw.core.states import Expr

        between_state = Between(expr=Column(name="age"), lower_bound=Column(name="18"), upper_bound=Column(name="65"))

        assert isinstance(between_state, Expr)


def describe_not_between_state() -> None:
    """Test NotBetween state dataclass."""

    def it_creates_with_expr_and_bounds() -> None:
        """NotBetween should store expr, lower_bound, and upper_bound."""
        expr = Column(name="age")
        lower = Column(name="0")
        upper = Column(name="17")
        not_between_state = NotBetween(expr=expr, lower_bound=lower, upper_bound=upper)

        assert not_between_state.expr == expr
        assert not_between_state.lower_bound == lower
        assert not_between_state.upper_bound == upper

    def it_is_frozen() -> None:
        """NotBetween should be immutable (frozen dataclass)."""
        import dataclasses

        import pytest

        not_between_state = NotBetween(
            expr=Column(name="age"), lower_bound=Column(name="0"), upper_bound=Column(name="17")
        )

        with pytest.raises(dataclasses.FrozenInstanceError):
            not_between_state.expr = Column(name="other")  # type: ignore

    def it_has_structural_equality() -> None:
        """Should use structural equality."""
        not_between1 = NotBetween(expr=Column(name="age"), lower_bound=Column(name="0"), upper_bound=Column(name="17"))
        not_between2 = NotBetween(expr=Column(name="age"), lower_bound=Column(name="0"), upper_bound=Column(name="17"))

        assert not_between1 is not not_between2
        assert not_between1 == not_between2

    def it_is_expr_subclass() -> None:
        """NotBetween should inherit from Expr base class."""
        from vw.core.states import Expr

        not_between_state = NotBetween(
            expr=Column(name="age"), lower_bound=Column(name="0"), upper_bound=Column(name="17")
        )

        assert isinstance(not_between_state, Expr)
