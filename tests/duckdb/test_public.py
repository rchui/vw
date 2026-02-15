"""Tests for vw.duckdb.public module - factory functions."""

from vw.core.case import When
from vw.core.states import (
    CTE,
    Column,
    Cube,
    Exists,
    GroupingSets,
    Parameter,
    Reference,
    Rollup,
    Values,
)
from vw.duckdb import render
from vw.duckdb.public import (
    F,
    col,
    cte,
    cube,
    exists,
    grouping_sets,
    lit,
    param,
    ref,
    rollup,
    values,
    when,
)


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


def test_values_factory() -> None:
    """Test values() creates a RowSet with Values state."""
    result = values("t", {"id": 1, "name": "Alice"})

    assert isinstance(result.state, Values)
    assert result.state.alias == "t"
    assert len(result.state.rows) == 1


def test_values_multiple_rows() -> None:
    """Test values() with multiple rows."""
    result = values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})

    assert isinstance(result.state, Values)
    assert len(result.state.rows) == 2


def test_values_rendering() -> None:
    """Test values() renders correctly."""
    query = values("t", {"id": 1, "name": "Alice"}).select(col("id"), col("name"))
    sql = render(query)

    assert sql.query == "SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
    assert sql.params == {"_v0_0_id": 1, "_v0_1_name": "Alice"}


def test_values_with_expressions() -> None:
    """Test values() with Expression objects."""
    result = values("t", {"id": lit(1), "name": lit("Alice")}, {"id": lit(2), "name": lit("Bob")})

    assert isinstance(result.state, Values)
    assert len(result.state.rows) == 2

    query = result.select(col("id"), col("name"))
    sql = render(query)

    assert sql.query == "SELECT id, name FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)"


def test_cte_factory() -> None:
    """Test cte() creates a RowSet with CTE state."""
    query = ref("users").select(col("id")).where(col("active") == lit(True))
    result = cte("active_users", query)

    assert isinstance(result.state, CTE)
    assert result.state.name == "active_users"
    assert result.state.recursive is False


def test_cte_recursive() -> None:
    """Test cte() with recursive=True."""
    anchor = ref("items").select(col("*")).where(col("parent_id").is_null())
    result = cte("tree", anchor, recursive=True)

    assert isinstance(result.state, CTE)
    assert result.state.recursive is True


def test_cte_rendering() -> None:
    """Test cte() renders correctly."""
    active_users = cte("active_users", ref("users").select(col("*")).where(col("active") == lit(True)))
    query = active_users.select(col("id"), col("name"))

    sql = render(query)
    assert "WITH active_users AS" in sql.query
    assert "SELECT id, name FROM active_users" in sql.query


def test_cte_with_reference() -> None:
    """Test cte() with a Reference (convenience wrapper)."""
    # Reference should be automatically wrapped with SELECT *
    result = cte("all_users", ref("users"))

    assert isinstance(result.state, CTE)
    assert result.state.name == "all_users"

    query = result.select(col("id"))
    sql = render(query)

    # DuckDB's Star renderer includes the table qualifier (users.*)
    assert "WITH all_users AS (SELECT users.* FROM users)" in sql.query
    assert "SELECT id FROM all_users" in sql.query


def test_rollup_factory() -> None:
    """Test rollup() creates an Expression with Rollup state."""
    result = rollup(col("region"), col("product"))

    assert isinstance(result.state, Rollup)
    assert len(result.state.columns) == 2


def test_rollup_rendering() -> None:
    """Test rollup() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(rollup(col("region"), col("product")))
    )

    sql = render(query)
    assert "GROUP BY ROLLUP (region, product)" in sql.query


def test_rollup_single_column() -> None:
    """Test rollup() with a single column."""
    query = ref("sales").select(col("region"), F.sum(col("amount")).alias("total")).group_by(rollup(col("region")))

    sql = render(query)
    assert "GROUP BY ROLLUP (region)" in sql.query


def test_cube_factory() -> None:
    """Test cube() creates an Expression with Cube state."""
    result = cube(col("region"), col("product"))

    assert isinstance(result.state, Cube)
    assert len(result.state.columns) == 2


def test_cube_rendering() -> None:
    """Test cube() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(cube(col("region"), col("product")))
    )

    sql = render(query)
    assert "GROUP BY CUBE (region, product)" in sql.query


def test_cube_single_column() -> None:
    """Test cube() with a single column."""
    query = ref("sales").select(col("region"), F.sum(col("amount")).alias("total")).group_by(cube(col("region")))

    sql = render(query)
    assert "GROUP BY CUBE (region)" in sql.query


def test_grouping_sets_factory() -> None:
    """Test grouping_sets() creates an Expression with GroupingSets state."""
    result = grouping_sets((col("region"),), (col("product"),), ())

    assert isinstance(result.state, GroupingSets)
    assert len(result.state.sets) == 3


def test_grouping_sets_rendering() -> None:
    """Test grouping_sets() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
    )

    sql = render(query)
    assert "GROUP BY GROUPING SETS ((region, product), (region), ())" in sql.query


def test_grouping_sets_empty_set() -> None:
    """Test grouping_sets() with an empty set (grand total)."""
    query = (
        ref("sales")
        .select(col("region"), F.sum(col("amount")).alias("total"))
        .group_by(grouping_sets((col("region"),), ()))
    )

    sql = render(query)
    assert "GROUP BY GROUPING SETS ((region), ())" in sql.query


def test_grouping_sets_multiple_columns() -> None:
    """Test grouping_sets() with multiple columns in sets."""
    query = (
        ref("sales")
        .select(col("year"), col("quarter"), col("region"), F.sum(col("amount")).alias("total"))
        .group_by(grouping_sets((col("year"), col("quarter")), (col("year"),), (col("region"),)))
    )

    sql = render(query)
    assert "GROUP BY GROUPING SETS ((year, quarter), (year), (region))" in sql.query
