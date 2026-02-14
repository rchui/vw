"""Tests for vw.duckdb.public module - factory functions."""

from vw.core.case import When
from vw.core.states import Column, Exists, Parameter, Reference
from vw.duckdb import render
from vw.duckdb.public import F, col, exists, lit, param, ref, when


def test_ref_factory() -> None:
    """Test ref() creates a RowSet with Reference state."""
    result = ref("users")

    assert isinstance(result.state, Reference)

    # Verify it renders correctly
    sql = render(result)
    assert sql.query == "FROM users"


def test_col_factory() -> None:
    """Test col() creates an Expression with Column state."""
    result = col("name")

    assert isinstance(result.state, Column)


def test_param_factory() -> None:
    """Test param() creates an Expression with Parameter state."""
    result = param("age", 18)

    assert isinstance(result.state, Parameter)


def test_lit_factory() -> None:
    """Test lit() creates a literal value and renders correctly."""
    # Verify it works in a query
    query = ref("test").select(lit(42).alias("num"))
    sql = render(query)
    assert sql.query == "SELECT 42 AS num FROM test"


def test_when_factory() -> None:
    """Test when() creates a When builder."""
    condition = col("age") >= lit(18)
    result = when(condition)

    assert isinstance(result, When)
    assert result.prior_whens == ()


def test_exists_factory() -> None:
    """Test exists() creates an Expression with Exists state."""
    # Use a Reference as subquery so it gets wrapped with SELECT *
    subquery_ref = ref("orders")
    result = exists(subquery_ref)

    assert isinstance(result.state, Exists)

    # Verify it renders correctly with Reference subquery
    query = ref("users").select(col("id")).where(result)
    sql = render(query)
    assert sql.query == "SELECT id FROM users WHERE EXISTS (SELECT * FROM orders)"


def test_exists_with_statement() -> None:
    """Test exists() with a Statement subquery."""
    subquery = ref("orders").where(col("user_id") == col("users.id"))
    result = exists(subquery)

    # Verify Statement subquery renders without SELECT wrapper
    query = ref("users").select(col("id")).where(result)
    sql = render(query)
    # Statement already has FROM, so just wrapped in parentheses
    assert sql.query == "SELECT id FROM users WHERE EXISTS (FROM orders WHERE user_id = users.id)"


def test_functions_instance() -> None:
    """Test F is a Functions instance with standard functions."""
    from vw.duckdb.public import Functions

    assert isinstance(F, Functions)
