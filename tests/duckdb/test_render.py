"""Basic DuckDB rendering tests."""

from vw.core.render import SQL
from vw.duckdb import F, col, lit, param, ref, render, when


def test_basic_select() -> None:
    """Test basic SELECT query rendering."""
    query = ref("users").select(col("id"), col("name"))
    result = render(query)

    assert result == SQL(query="SELECT id, name FROM users", params={})


def test_where_with_param() -> None:
    """Test WHERE clause with parameter."""
    query = ref("users").select(col("id")).where(col("age") >= param("min_age", 18))
    result = render(query)

    assert result == SQL(query="SELECT id FROM users WHERE age >= $min_age", params={"min_age": 18})


def test_aggregate_functions() -> None:
    """Test aggregate functions."""
    query = ref("orders").select(F.count(), F.sum(col("amount")).alias("total"))
    result = render(query)

    assert result == SQL(query="SELECT COUNT(*), SUM(amount) AS total FROM orders", params={})


def test_case_when() -> None:
    """Test CASE WHEN expression."""
    query = ref("users").select(
        when(col("age") >= lit(18)).then(lit("adult")).otherwise(lit("minor")).alias("category")
    )
    result = render(query)

    assert result == SQL(
        query="SELECT CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS category FROM users",
        params={},
    )


def test_joins() -> None:
    """Test JOIN operations."""
    users = ref("users").alias("u")
    orders = ref("orders").alias("o")

    query = users.select(col("u.name"), col("o.total")).join.inner(orders, on=[(col("u.id") == col("o.user_id"))])
    result = render(query)

    assert result == SQL(
        query="SELECT u.name, o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)",
        params={},
    )


def test_window_functions() -> None:
    """Test window functions."""
    query = ref("sales").select(
        col("product"), F.row_number().over(partition_by=[col("category")], order_by=[col("amount").desc()])
    )
    result = render(query)

    assert result == SQL(
        query="SELECT product, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) FROM sales",
        params={},
    )


def test_qualify_clause() -> None:
    """Test QUALIFY clause rendering (DuckDB-specific post-window filter)."""
    query = (
        ref("employees")
        .select(
            col("dept"),
            col("name"),
            F.row_number().over(partition_by=[col("dept")], order_by=[col("salary").desc()]).alias("rn"),
        )
        .qualify(col("rn") == param("rank", 1))
    )
    result = render(query)

    assert result == SQL(
        query="SELECT dept, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees QUALIFY rn = $rank",
        params={"rank": 1},
    )


def test_qualify_before_order_by() -> None:
    """QUALIFY must render before the top-level ORDER BY."""
    query = (
        ref("t")
        .select(col("id"), F.row_number().over(order_by=[col("id")]).alias("rn"))
        .qualify(col("rn") == param("n", 1))
        .order_by(col("id"))
    )
    result = render(query)

    # rfind locates the top-level ORDER BY, not the one inside the window function spec
    assert "QUALIFY" in result.query
    assert result.query.index("QUALIFY") < result.query.rfind("ORDER BY")
