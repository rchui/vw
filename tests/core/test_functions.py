"""Tests for core Functions class (ANSI SQL aggregate and window functions)."""

from vw.core.states import (
    Column,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Function,
    Literal,
    WindowFunction,
)
from vw.postgres import F, col, lit, param, ref, render
from vw.postgres.base import Expression


def describe_functions_class() -> None:
    """Test Functions class initialization."""

    def it_initializes_with_factories() -> None:
        """Functions should be initialized with factories."""
        assert F is not None
        assert hasattr(F, "count")
        assert hasattr(F, "sum")
        assert hasattr(F, "row_number")


# --- Aggregate Functions --------------------------------------------------- #


def describe_count_function() -> None:
    """Test F.count() aggregate function."""

    def it_returns_expression() -> None:
        """count() should return an Expression."""
        expr = F.count()
        assert isinstance(expr, Expression)

    def it_creates_function_state_for_count_star() -> None:
        """count() without args should create COUNT(*) function state."""
        expr = F.count()
        assert isinstance(expr.state, Function)
        assert expr.state.name == "COUNT"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "*"
        assert expr.state.distinct is False

    def it_creates_function_state_for_count_expr() -> None:
        """count(expr) should create COUNT(expr) function state."""
        expr = F.count(col("id"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "COUNT"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "id"
        assert expr.state.distinct is False

    def it_supports_distinct() -> None:
        """count(expr, distinct=True) should set distinct flag."""
        expr = F.count(col("user_id"), distinct=True)
        assert isinstance(expr.state, Function)
        assert expr.state.name == "COUNT"
        assert expr.state.distinct is True

    def it_renders_count_star() -> None:
        """COUNT(*) should render correctly."""
        expr = F.count()
        result = render(expr)
        assert result.query == "COUNT(*)"
        assert result.params == {}

    def it_renders_count_column() -> None:
        """COUNT(column) should render correctly."""
        expr = F.count(col("id"))
        result = render(expr)
        assert result.query == "COUNT(id)"
        assert result.params == {}

    def it_renders_count_distinct() -> None:
        """COUNT(DISTINCT column) should render correctly."""
        expr = F.count(col("user_id"), distinct=True)
        result = render(expr)
        assert result.query == "COUNT(DISTINCT user_id)"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """COUNT with alias should render correctly."""
        expr = F.count().alias("total")
        result = render(expr)
        assert result.query == "COUNT(*) AS total"
        assert result.params == {}


def describe_sum_function() -> None:
    """Test F.sum() aggregate function."""

    def it_returns_expression() -> None:
        """sum() should return an Expression."""
        expr = F.sum(col("amount"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """sum(expr) should create SUM function state."""
        expr = F.sum(col("amount"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "SUM"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "amount"

    def it_renders_sum() -> None:
        """SUM(column) should render correctly."""
        expr = F.sum(col("amount"))
        result = render(expr)
        assert result.query == "SUM(amount)"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """SUM with alias should render correctly."""
        expr = F.sum(col("revenue")).alias("total_revenue")
        result = render(expr)
        assert result.query == "SUM(revenue) AS total_revenue"
        assert result.params == {}


def describe_avg_function() -> None:
    """Test F.avg() aggregate function."""

    def it_returns_expression() -> None:
        """avg() should return an Expression."""
        expr = F.avg(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """avg(expr) should create AVG function state."""
        expr = F.avg(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "AVG"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_renders_avg() -> None:
        """AVG(column) should render correctly."""
        expr = F.avg(col("price"))
        result = render(expr)
        assert result.query == "AVG(price)"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """AVG with alias should render correctly."""
        expr = F.avg(col("score")).alias("avg_score")
        result = render(expr)
        assert result.query == "AVG(score) AS avg_score"
        assert result.params == {}


def describe_min_function() -> None:
    """Test F.min() aggregate function."""

    def it_returns_expression() -> None:
        """min() should return an Expression."""
        expr = F.min(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """min(expr) should create MIN function state."""
        expr = F.min(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "MIN"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_renders_min() -> None:
        """MIN(column) should render correctly."""
        expr = F.min(col("price"))
        result = render(expr)
        assert result.query == "MIN(price)"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """MIN with alias should render correctly."""
        expr = F.min(col("created_at")).alias("earliest")
        result = render(expr)
        assert result.query == "MIN(created_at) AS earliest"
        assert result.params == {}


def describe_max_function() -> None:
    """Test F.max() aggregate function."""

    def it_returns_expression() -> None:
        """max() should return an Expression."""
        expr = F.max(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """max(expr) should create MAX function state."""
        expr = F.max(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "MAX"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_renders_max() -> None:
        """MAX(column) should render correctly."""
        expr = F.max(col("price"))
        result = render(expr)
        assert result.query == "MAX(price)"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """MAX with alias should render correctly."""
        expr = F.max(col("updated_at")).alias("latest")
        result = render(expr)
        assert result.query == "MAX(updated_at) AS latest"
        assert result.params == {}


# --- Window Functions ------------------------------------------------------ #


def describe_row_number_function() -> None:
    """Test F.row_number() window function."""

    def it_returns_expression() -> None:
        """row_number() should return an Expression."""
        expr = F.row_number()
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """row_number() should create ROW_NUMBER function state."""
        expr = F.row_number()
        assert isinstance(expr.state, Function)
        assert expr.state.name == "ROW_NUMBER"
        assert len(expr.state.args) == 0

    def it_renders_with_over() -> None:
        """ROW_NUMBER() with .over() should render correctly."""
        expr = F.row_number().over(order_by=[col("created_at").desc()])
        result = render(expr)
        assert result.query == "ROW_NUMBER() OVER (ORDER BY created_at DESC)"
        assert result.params == {}

    def it_renders_with_partition_and_order() -> None:
        """ROW_NUMBER() with partition and order should render correctly."""
        expr = F.row_number().over(partition_by=[col("category")], order_by=[col("price").desc()])
        result = render(expr)
        assert result.query == "ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC)"
        assert result.params == {}

    def it_creates_window_function_state() -> None:
        """row_number().over() should create WindowFunction state."""
        expr = F.row_number().over(order_by=[col("id")])
        assert isinstance(expr.state, WindowFunction)
        assert isinstance(expr.state.function, Function)
        assert expr.state.function.name == "ROW_NUMBER"


def describe_rank_function() -> None:
    """Test F.rank() window function."""

    def it_returns_expression() -> None:
        """rank() should return an Expression."""
        expr = F.rank()
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """rank() should create RANK function state."""
        expr = F.rank()
        assert isinstance(expr.state, Function)
        assert expr.state.name == "RANK"
        assert len(expr.state.args) == 0

    def it_renders_with_over() -> None:
        """RANK() with .over() should render correctly."""
        expr = F.rank().over(order_by=[col("score").desc()])
        result = render(expr)
        assert result.query == "RANK() OVER (ORDER BY score DESC)"
        assert result.params == {}


def describe_dense_rank_function() -> None:
    """Test F.dense_rank() window function."""

    def it_returns_expression() -> None:
        """dense_rank() should return an Expression."""
        expr = F.dense_rank()
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """dense_rank() should create DENSE_RANK function state."""
        expr = F.dense_rank()
        assert isinstance(expr.state, Function)
        assert expr.state.name == "DENSE_RANK"
        assert len(expr.state.args) == 0

    def it_renders_with_over() -> None:
        """DENSE_RANK() with .over() should render correctly."""
        expr = F.dense_rank().over(order_by=[col("score").desc()])
        result = render(expr)
        assert result.query == "DENSE_RANK() OVER (ORDER BY score DESC)"
        assert result.params == {}


def describe_ntile_function() -> None:
    """Test F.ntile() window function."""

    def it_returns_expression() -> None:
        """ntile(n) should return an Expression."""
        expr = F.ntile(4)
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_literal() -> None:
        """ntile(n) should create NTILE function state with literal argument."""
        expr = F.ntile(4)
        assert isinstance(expr.state, Function)
        assert expr.state.name == "NTILE"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Literal)
        assert expr.state.args[0].value == 4

    def it_renders_with_over() -> None:
        """NTILE(n) with .over() should render correctly."""
        expr = F.ntile(4).over(order_by=[col("salary").desc()])
        result = render(expr)
        assert result.query == "NTILE(4) OVER (ORDER BY salary DESC)"
        assert result.params == {}

    def it_supports_different_bucket_counts() -> None:
        """NTILE should support different bucket counts."""
        expr = F.ntile(10).over(order_by=[col("score")])
        result = render(expr)
        assert result.query == "NTILE(10) OVER (ORDER BY score)"
        assert result.params == {}


def describe_lag_function() -> None:
    """Test F.lag() offset window function."""

    def it_returns_expression() -> None:
        """lag() should return an Expression."""
        expr = F.lag(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_default_offset() -> None:
        """lag(expr) should create LAG function state with default offset."""
        expr = F.lag(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LAG"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_creates_function_state_with_custom_offset() -> None:
        """lag(expr, offset) should create LAG function state with custom offset."""
        expr = F.lag(col("price"), 2)
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LAG"
        assert len(expr.state.args) == 2
        assert isinstance(expr.state.args[0], Column)
        assert isinstance(expr.state.args[1], Literal)
        assert expr.state.args[1].value == 2

    def it_creates_function_state_with_default_value() -> None:
        """lag(expr, offset, default) should include default value."""
        expr = F.lag(col("price"), 1, lit(0))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LAG"
        assert len(expr.state.args) == 3
        assert isinstance(expr.state.args[2], Literal)
        assert expr.state.args[2].value == 0

    def it_renders_lag_with_default_offset() -> None:
        """LAG(expr) should render correctly."""
        expr = F.lag(col("price")).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LAG(price) OVER (ORDER BY date)"
        assert result.params == {}

    def it_renders_lag_with_custom_offset() -> None:
        """LAG(expr, offset) should render correctly."""
        expr = F.lag(col("price"), 2).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LAG(price, 2) OVER (ORDER BY date)"
        assert result.params == {}

    def it_renders_lag_with_default_value() -> None:
        """LAG(expr, offset, default) should render correctly."""
        expr = F.lag(col("price"), 1, lit(0)).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LAG(price, 1, 0) OVER (ORDER BY date)"
        assert result.params == {}

    def it_renders_lag_with_param_default() -> None:
        """LAG with parameter default should render correctly."""
        expr = F.lag(col("price"), 1, param("default_price", 0)).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LAG(price, 1, $default_price) OVER (ORDER BY date)"
        assert result.params == {"default_price": 0}


def describe_lead_function() -> None:
    """Test F.lead() offset window function."""

    def it_returns_expression() -> None:
        """lead() should return an Expression."""
        expr = F.lead(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_default_offset() -> None:
        """lead(expr) should create LEAD function state with default offset."""
        expr = F.lead(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LEAD"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_creates_function_state_with_custom_offset() -> None:
        """lead(expr, offset) should create LEAD function state with custom offset."""
        expr = F.lead(col("price"), 3)
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LEAD"
        assert len(expr.state.args) == 2
        assert isinstance(expr.state.args[0], Column)
        assert isinstance(expr.state.args[1], Literal)
        assert expr.state.args[1].value == 3

    def it_creates_function_state_with_default_value() -> None:
        """lead(expr, offset, default) should include default value."""
        expr = F.lead(col("price"), 1, lit(0))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LEAD"
        assert len(expr.state.args) == 3
        assert isinstance(expr.state.args[2], Literal)
        assert expr.state.args[2].value == 0

    def it_renders_lead_with_default_offset() -> None:
        """LEAD(expr) should render correctly."""
        expr = F.lead(col("price")).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LEAD(price) OVER (ORDER BY date)"
        assert result.params == {}

    def it_renders_lead_with_custom_offset() -> None:
        """LEAD(expr, offset) should render correctly."""
        expr = F.lead(col("price"), 2).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LEAD(price, 2) OVER (ORDER BY date)"
        assert result.params == {}

    def it_renders_lead_with_default_value() -> None:
        """LEAD(expr, offset, default) should render correctly."""
        expr = F.lead(col("price"), 1, lit(0)).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LEAD(price, 1, 0) OVER (ORDER BY date)"
        assert result.params == {}


def describe_first_value_function() -> None:
    """Test F.first_value() window function."""

    def it_returns_expression() -> None:
        """first_value() should return an Expression."""
        expr = F.first_value(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """first_value(expr) should create FIRST_VALUE function state."""
        expr = F.first_value(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "FIRST_VALUE"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_renders_with_over() -> None:
        """FIRST_VALUE(expr) with .over() should render correctly."""
        expr = F.first_value(col("price")).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "FIRST_VALUE(price) OVER (ORDER BY date)"
        assert result.params == {}


def describe_last_value_function() -> None:
    """Test F.last_value() window function."""

    def it_returns_expression() -> None:
        """last_value() should return an Expression."""
        expr = F.last_value(col("price"))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """last_value(expr) should create LAST_VALUE function state."""
        expr = F.last_value(col("price"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LAST_VALUE"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "price"

    def it_renders_with_over() -> None:
        """LAST_VALUE(expr) with .over() should render correctly."""
        expr = F.last_value(col("price")).over(order_by=[col("date")])
        result = render(expr)
        assert result.query == "LAST_VALUE(price) OVER (ORDER BY date)"
        assert result.params == {}


# --- Date/Time Functions --------------------------------------------------- #


def describe_current_timestamp_function() -> None:
    """Test F.current_timestamp() date/time function."""

    def it_returns_expression() -> None:
        """current_timestamp() should return an Expression."""
        expr = F.current_timestamp()
        assert isinstance(expr, Expression)

    def it_creates_current_timestamp_state() -> None:
        """current_timestamp() should create CurrentTimestamp state."""
        expr = F.current_timestamp()
        assert isinstance(expr.state, CurrentTimestamp)

    def it_renders_current_timestamp() -> None:
        """CURRENT_TIMESTAMP should render correctly."""
        expr = F.current_timestamp()
        result = render(expr)
        assert result.query == "CURRENT_TIMESTAMP"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_TIMESTAMP with alias should render correctly."""
        expr = F.current_timestamp().alias("now")
        result = render(expr)
        assert result.query == "CURRENT_TIMESTAMP AS now"
        assert result.params == {}


def describe_current_date_function() -> None:
    """Test F.current_date() date/time function."""

    def it_returns_expression() -> None:
        """current_date() should return an Expression."""
        expr = F.current_date()
        assert isinstance(expr, Expression)

    def it_creates_current_date_state() -> None:
        """current_date() should create CurrentDate state."""
        expr = F.current_date()
        assert isinstance(expr.state, CurrentDate)

    def it_renders_current_date() -> None:
        """CURRENT_DATE should render correctly."""
        expr = F.current_date()
        result = render(expr)
        assert result.query == "CURRENT_DATE"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_DATE with alias should render correctly."""
        expr = F.current_date().alias("today")
        result = render(expr)
        assert result.query == "CURRENT_DATE AS today"
        assert result.params == {}


def describe_current_time_function() -> None:
    """Test F.current_time() date/time function."""

    def it_returns_expression() -> None:
        """current_time() should return an Expression."""
        expr = F.current_time()
        assert isinstance(expr, Expression)

    def it_creates_current_time_state() -> None:
        """current_time() should create CurrentTime state."""
        expr = F.current_time()
        assert isinstance(expr.state, CurrentTime)

    def it_renders_current_time() -> None:
        """CURRENT_TIME should render correctly."""
        expr = F.current_time()
        result = render(expr)
        assert result.query == "CURRENT_TIME"
        assert result.params == {}

    def it_renders_with_alias() -> None:
        """CURRENT_TIME with alias should render correctly."""
        expr = F.current_time().alias("time_now")
        result = render(expr)
        assert result.query == "CURRENT_TIME AS time_now"
        assert result.params == {}


# --- Grouping Functions ---------------------------------------------------- #


def describe_grouping_function() -> None:
    """Test F.grouping() grouping function."""

    def it_returns_expression() -> None:
        """grouping() should return an Expression."""
        expr = F.grouping(col("region"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_single_arg() -> None:
        """grouping(expr) should create GROUPING function state."""
        expr = F.grouping(col("region"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "GROUPING"
        assert len(expr.state.args) == 1
        assert isinstance(expr.state.args[0], Column)
        assert expr.state.args[0].name == "region"

    def it_creates_function_state_with_multiple_args() -> None:
        """grouping(*exprs) should create GROUPING function state with multiple args."""
        expr = F.grouping(col("region"), col("product"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "GROUPING"
        assert len(expr.state.args) == 2
        assert isinstance(expr.state.args[0], Column)
        assert isinstance(expr.state.args[1], Column)
        assert expr.state.args[0].name == "region"
        assert expr.state.args[1].name == "product"

    def it_renders_grouping_single_arg() -> None:
        """GROUPING(col) should render correctly."""
        expr = F.grouping(col("region"))
        result = render(expr)
        assert result.query == "GROUPING(region)"
        assert result.params == {}

    def it_renders_grouping_multiple_args() -> None:
        """GROUPING(col1, col2) should render correctly."""
        expr = F.grouping(col("region"), col("product"))
        result = render(expr)
        assert result.query == "GROUPING(region, product)"
        assert result.params == {}


# --- Null Handling Functions ----------------------------------------------- #


def describe_coalesce_function() -> None:
    """Test F.coalesce() null handling function."""

    def it_returns_expression() -> None:
        """coalesce() should return an Expression."""
        expr = F.coalesce(col("nickname"), col("name"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_two_args() -> None:
        """coalesce(expr1, expr2) should create COALESCE function state."""
        expr = F.coalesce(col("nickname"), col("name"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "COALESCE"
        assert len(expr.state.args) == 2
        assert isinstance(expr.state.args[0], Column)
        assert isinstance(expr.state.args[1], Column)

    def it_creates_function_state_with_multiple_args() -> None:
        """coalesce(*exprs) should create COALESCE function state with multiple args."""
        expr = F.coalesce(col("a"), col("b"), col("c"), lit("default"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "COALESCE"
        assert len(expr.state.args) == 4

    def it_renders_coalesce_two_args() -> None:
        """COALESCE(col1, col2) should render correctly."""
        expr = F.coalesce(col("nickname"), col("name"))
        result = render(expr)
        assert result.query == "COALESCE(nickname, name)"
        assert result.params == {}

    def it_renders_coalesce_multiple_args() -> None:
        """COALESCE with multiple args should render correctly."""
        expr = F.coalesce(col("a"), col("b"), lit("default"))
        result = render(expr)
        assert result.query == "COALESCE(a, b, 'default')"
        assert result.params == {}


def describe_nullif_function() -> None:
    """Test F.nullif() null handling function."""

    def it_returns_expression() -> None:
        """nullif() should return an Expression."""
        expr = F.nullif(col("value"), lit(0))
        assert isinstance(expr, Expression)

    def it_creates_function_state() -> None:
        """nullif(expr1, expr2) should create NULLIF function state."""
        expr = F.nullif(col("value"), lit(0))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "NULLIF"
        assert len(expr.state.args) == 2
        assert isinstance(expr.state.args[0], Column)
        assert isinstance(expr.state.args[1], Literal)

    def it_renders_nullif() -> None:
        """NULLIF(expr1, expr2) should render correctly."""
        expr = F.nullif(col("value"), lit(0))
        result = render(expr)
        assert result.query == "NULLIF(value, 0)"
        assert result.params == {}

    def it_renders_nullif_with_param() -> None:
        """NULLIF with parameter should render correctly."""
        expr = F.nullif(col("value"), param("sentinel", 0))
        result = render(expr)
        assert result.query == "NULLIF(value, $sentinel)"
        assert result.params == {"sentinel": 0}


def describe_greatest_function() -> None:
    """Test F.greatest() comparison function."""

    def it_returns_expression() -> None:
        """greatest() should return an Expression."""
        expr = F.greatest(col("a"), col("b"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_two_args() -> None:
        """greatest(expr1, expr2) should create GREATEST function state."""
        expr = F.greatest(col("a"), col("b"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "GREATEST"
        assert len(expr.state.args) == 2

    def it_creates_function_state_with_multiple_args() -> None:
        """greatest(*exprs) should create GREATEST function state with multiple args."""
        expr = F.greatest(col("a"), col("b"), col("c"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "GREATEST"
        assert len(expr.state.args) == 3

    def it_renders_greatest_two_args() -> None:
        """GREATEST(col1, col2) should render correctly."""
        expr = F.greatest(col("a"), col("b"))
        result = render(expr)
        assert result.query == "GREATEST(a, b)"
        assert result.params == {}

    def it_renders_greatest_multiple_args() -> None:
        """GREATEST with multiple args should render correctly."""
        expr = F.greatest(col("a"), col("b"), col("c"))
        result = render(expr)
        assert result.query == "GREATEST(a, b, c)"
        assert result.params == {}


def describe_least_function() -> None:
    """Test F.least() comparison function."""

    def it_returns_expression() -> None:
        """least() should return an Expression."""
        expr = F.least(col("a"), col("b"))
        assert isinstance(expr, Expression)

    def it_creates_function_state_with_two_args() -> None:
        """least(expr1, expr2) should create LEAST function state."""
        expr = F.least(col("a"), col("b"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LEAST"
        assert len(expr.state.args) == 2

    def it_creates_function_state_with_multiple_args() -> None:
        """least(*exprs) should create LEAST function state with multiple args."""
        expr = F.least(col("a"), col("b"), col("c"))
        assert isinstance(expr.state, Function)
        assert expr.state.name == "LEAST"
        assert len(expr.state.args) == 3

    def it_renders_least_two_args() -> None:
        """LEAST(col1, col2) should render correctly."""
        expr = F.least(col("a"), col("b"))
        result = render(expr)
        assert result.query == "LEAST(a, b)"
        assert result.params == {}

    def it_renders_least_multiple_args() -> None:
        """LEAST with multiple args should render correctly."""
        expr = F.least(col("a"), col("b"), col("c"))
        result = render(expr)
        assert result.query == "LEAST(a, b, c)"
        assert result.params == {}


# --- Functions in SELECT Clauses ------------------------------------------- #


def describe_aggregates_in_select() -> None:
    """Test aggregate functions in SELECT clauses."""

    def it_renders_count_in_select() -> None:
        """COUNT in SELECT should render correctly."""
        expr = ref("users").select(F.count().alias("total"))
        result = render(expr)
        assert "COUNT(*) AS total" in result.query

    def it_renders_sum_in_select() -> None:
        """SUM in SELECT should render correctly."""
        expr = ref("orders").select(F.sum(col("amount")).alias("total_amount"))
        result = render(expr)
        assert "SUM(amount) AS total_amount" in result.query

    def it_renders_multiple_aggregates() -> None:
        """Multiple aggregates in SELECT should render correctly."""
        expr = ref("products").select(
            F.count().alias("count"), F.avg(col("price")).alias("avg_price"), F.max(col("price")).alias("max_price")
        )
        result = render(expr)
        assert "COUNT(*) AS count" in result.query
        assert "AVG(price) AS avg_price" in result.query
        assert "MAX(price) AS max_price" in result.query


def describe_window_functions_in_select() -> None:
    """Test window functions in SELECT clauses."""

    def it_renders_row_number_in_select() -> None:
        """ROW_NUMBER in SELECT should render correctly."""
        expr = ref("events").select(col("id"), F.row_number().over(order_by=[col("created_at")]).alias("row_num"))
        result = render(expr)
        assert "ROW_NUMBER() OVER (ORDER BY created_at) AS row_num" in result.query

    def it_renders_lag_in_select() -> None:
        """LAG in SELECT should render correctly."""
        expr = ref("prices").select(
            col("date"), col("price"), F.lag(col("price")).over(order_by=[col("date")]).alias("prev_price")
        )
        result = render(expr)
        assert "LAG(price) OVER (ORDER BY date) AS prev_price" in result.query

    def it_renders_multiple_window_functions() -> None:
        """Multiple window functions in SELECT should render correctly."""
        expr = ref("sales").select(
            col("product"),
            F.row_number().over(partition_by=[col("category")], order_by=[col("amount").desc()]).alias("rank"),
            F.sum(col("amount")).over(partition_by=[col("category")]).alias("category_total"),
        )
        result = render(expr)
        assert "ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rank" in result.query
        assert "SUM(amount) OVER (PARTITION BY category) AS category_total" in result.query


def describe_datetime_functions_in_select() -> None:
    """Test date/time functions in SELECT clauses."""

    def it_renders_current_timestamp_in_select() -> None:
        """CURRENT_TIMESTAMP in SELECT should render correctly."""
        expr = ref("events").select(col("id"), F.current_timestamp().alias("fetched_at"))
        result = render(expr)
        assert "CURRENT_TIMESTAMP AS fetched_at" in result.query

    def it_renders_current_date_in_select() -> None:
        """CURRENT_DATE in SELECT should render correctly."""
        expr = ref("logs").select(col("id"), F.current_date().alias("today"))
        result = render(expr)
        assert "CURRENT_DATE AS today" in result.query


# --- Aggregate Functions as Window Functions ------------------------------ #


def describe_aggregates_as_window_functions() -> None:
    """Test aggregate functions used as window functions."""

    def it_renders_sum_over_partition() -> None:
        """SUM() OVER (PARTITION BY) should render correctly."""
        expr = F.sum(col("amount")).over(partition_by=[col("customer_id")])
        result = render(expr)
        assert result.query == "SUM(amount) OVER (PARTITION BY customer_id)"
        assert result.params == {}

    def it_renders_count_over_partition() -> None:
        """COUNT() OVER (PARTITION BY) should render correctly."""
        expr = F.count().over(partition_by=[col("category")])
        result = render(expr)
        assert result.query == "COUNT(*) OVER (PARTITION BY category)"
        assert result.params == {}

    def it_renders_avg_over_partition_and_order() -> None:
        """AVG() OVER (PARTITION BY ... ORDER BY) should render correctly."""
        expr = F.avg(col("price")).over(partition_by=[col("category")], order_by=[col("date")])
        result = render(expr)
        assert result.query == "AVG(price) OVER (PARTITION BY category ORDER BY date)"
        assert result.params == {}

    def it_creates_window_function_state() -> None:
        """Aggregate with .over() should create WindowFunction state."""
        expr = F.sum(col("amount")).over(partition_by=[col("customer_id")])
        assert isinstance(expr.state, WindowFunction)
        assert isinstance(expr.state.function, Function)
        assert expr.state.function.name == "SUM"
