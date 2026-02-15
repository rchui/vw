"""Tests for vw.postgres.public module - factory functions."""

from dataclasses import asdict

from vw.core.case import When
from vw.core.states import (
    CTE,
    Column,
    Cube,
    Exists,
    GroupingSets,
    Literal,
    Parameter,
    Reference,
    Rollup,
    Values,
)
from vw.postgres import render
from vw.postgres.public import (
    F,
    col,
    cte,
    cube,
    exists,
    grouping_sets,
    interval,
    lit,
    param,
    ref,
    rollup,
    values,
    when,
)
from vw.postgres.states import Interval


def test_ref_factory() -> None:
    """Test ref() creates a RowSet with Reference state."""
    result = ref("users")

    assert asdict(result.state) == asdict(Reference(name="users", alias=None, modifiers=()))

    # Verify it renders correctly
    sql = render(result)
    assert sql.query == "FROM users"


def test_ref_with_alias() -> None:
    """Test ref() with alias."""
    result = ref("users").alias("u")

    assert asdict(result.state) == asdict(Reference(name="users", alias="u", modifiers=()))

    sql = render(result)
    assert sql.query == "FROM users AS u"


def test_col_factory() -> None:
    """Test col() creates an Expression with Column state."""
    result = col("name")

    assert asdict(result.state) == asdict(Column(name="name", alias=None))


def test_col_qualified() -> None:
    """Test col() with qualified name."""
    result = col("users.name")

    assert asdict(result.state) == asdict(Column(name="users.name", alias=None))

    sql = render(result)
    assert sql.query == "users.name"


def test_param_factory() -> None:
    """Test param() creates an Expression with Parameter state."""
    result = param("age", 18)

    assert asdict(result.state) == asdict(Parameter(name="age", value=18))


def test_param_in_query() -> None:
    """Test param() renders correctly in a query."""
    query = ref("users").select(col("id")).where(col("age") >= param("min_age", 18))
    sql = render(query)

    assert sql.query == "SELECT id FROM users WHERE age >= $min_age"
    assert sql.params == {"min_age": 18}


def test_lit_factory() -> None:
    """Test lit() creates a literal value expression."""
    result = lit(42)

    assert asdict(result.state) == asdict(Literal(value=42))


def test_lit_string() -> None:
    """Test lit() with string value."""
    query = ref("test").select(lit("hello").alias("msg"))
    sql = render(query)

    assert sql.query == "SELECT 'hello' AS msg FROM test"
    assert sql.params == {}


def test_lit_number() -> None:
    """Test lit() with numeric value."""
    query = ref("test").select(lit(42).alias("num"))
    sql = render(query)

    assert sql.query == "SELECT 42 AS num FROM test"


def test_lit_boolean() -> None:
    """Test lit() with boolean values."""
    query = ref("test").select(lit(True).alias("t"), lit(False).alias("f"))
    sql = render(query)

    assert sql.query == "SELECT TRUE AS t, FALSE AS f FROM test"


def test_lit_null() -> None:
    """Test lit() with None (NULL)."""
    query = ref("test").select(lit(None).alias("n"))
    sql = render(query)

    assert sql.query == "SELECT NULL AS n FROM test"


def test_when_factory() -> None:
    """Test when() creates a When builder."""
    condition = col("age") >= lit(18)
    result = when(condition)

    assert isinstance(result, When)
    assert result.prior_whens == ()
    # When is a builder, not a state, so we verify its key attributes


def test_when_then_otherwise() -> None:
    """Test when().then().otherwise() constructs a CASE expression."""
    expr = when(col("status") == lit("active")).then(lit(1)).otherwise(lit(0))

    query = ref("users").select(expr.alias("status_num"))
    sql = render(query)

    assert sql.query == "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS status_num FROM users"


def test_exists_factory() -> None:
    """Test exists() creates an Expression with Exists state."""
    subquery_ref = ref("orders")
    result = exists(subquery_ref)

    assert isinstance(result.state, Exists)


def test_exists_in_where() -> None:
    """Test exists() in WHERE clause."""
    subquery = ref("orders").where(col("user_id") == col("users.id"))
    query = ref("users").select(col("id")).where(exists(subquery))

    sql = render(query)
    assert sql.query == "SELECT id FROM users WHERE EXISTS (FROM orders WHERE user_id = users.id)"


def test_values_factory() -> None:
    """Test values() creates a RowSet with Values state."""
    result = values("t", {"id": 1, "name": "Alice"})

    assert asdict(result.state) == asdict(Values(rows=({"id": 1, "name": "Alice"},), alias="t", modifiers=()))


def test_values_multiple_rows() -> None:
    """Test values() with multiple rows."""
    result = values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})

    assert asdict(result.state) == asdict(
        Values(rows=({"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}), alias="t", modifiers=())
    )


def test_values_rendering() -> None:
    """Test values() renders correctly."""
    query = values("t", {"id": 1, "name": "Alice"}).select(col("id"), col("name"))
    sql = render(query)

    assert sql.query == "SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
    assert sql.params == {"_v0_0_id": 1, "_v0_1_name": "Alice"}


def test_cte_factory() -> None:
    """Test cte() creates a RowSet with CTE state."""
    from vw.core.states import Operator

    query = ref("users").select(col("id")).where(col("active") == lit(True))
    result = cte("active_users", query)

    # CTE has complex nested state, so we verify key attributes
    assert isinstance(result.state, CTE)
    assert result.state.name == "active_users"
    assert result.state.recursive is False
    assert asdict(result.state.source) == asdict(Reference(name="users", alias=None, modifiers=()))
    assert len(result.state.columns) == 1
    assert asdict(result.state.columns[0]) == asdict(Column(name="id", alias=None))
    assert len(result.state.where_conditions) == 1
    assert isinstance(result.state.where_conditions[0], Operator)


def test_cte_recursive() -> None:
    """Test cte() with recursive=True."""
    anchor = ref("items").select(col("*")).where(col("parent_id").is_null())
    result = cte("tree", anchor, recursive=True)

    assert isinstance(result.state, CTE)
    assert result.state.recursive is True
    assert result.state.name == "tree"


def test_cte_rendering() -> None:
    """Test cte() renders correctly."""
    active_users = cte("active_users", ref("users").select(col("*")).where(col("active") == lit(True)))
    query = active_users.select(col("id"), col("name"))

    sql = render(query)
    assert "WITH active_users AS" in sql.query
    assert "SELECT id, name FROM active_users" in sql.query


def test_rollup_factory() -> None:
    """Test rollup() creates an Expression with Rollup state."""
    result = rollup(col("region"), col("product"))

    assert asdict(result.state) == asdict(
        Rollup(columns=(Column(name="region", alias=None), Column(name="product", alias=None)))
    )


def test_rollup_rendering() -> None:
    """Test rollup() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(rollup(col("region"), col("product")))
    )

    sql = render(query)
    assert "GROUP BY ROLLUP (region, product)" in sql.query


def test_cube_factory() -> None:
    """Test cube() creates an Expression with Cube state."""
    result = cube(col("region"), col("product"))

    assert asdict(result.state) == asdict(
        Cube(columns=(Column(name="region", alias=None), Column(name="product", alias=None)))
    )


def test_cube_rendering() -> None:
    """Test cube() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(cube(col("region"), col("product")))
    )

    sql = render(query)
    assert "GROUP BY CUBE (region, product)" in sql.query


def test_grouping_sets_factory() -> None:
    """Test grouping_sets() creates an Expression with GroupingSets state."""
    result = grouping_sets((col("region"),), (col("product"),), ())

    assert asdict(result.state) == asdict(
        GroupingSets(sets=((Column(name="region", alias=None),), (Column(name="product", alias=None),), ()))
    )


def test_grouping_sets_rendering() -> None:
    """Test grouping_sets() renders correctly in GROUP BY."""
    query = (
        ref("sales")
        .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
        .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
    )

    sql = render(query)
    assert "GROUP BY GROUPING SETS ((region, product), (region), ())" in sql.query


def test_interval_factory() -> None:
    """Test interval() creates an Expression with Interval state."""
    result = interval(1, "day")

    assert asdict(result.state) == asdict(Interval(amount=1, unit="day"))


def test_interval_addition() -> None:
    """Test interval() in addition expression."""
    query = ref("events").select((col("created_at") + interval(1, "day")).alias("next_day"))

    sql = render(query)
    assert "created_at + INTERVAL '1 day'" in sql.query


def test_interval_subtraction() -> None:
    """Test interval() in subtraction expression."""
    query = ref("events").select((col("expires_at") - interval(30, "day")).alias("grace_period"))

    sql = render(query)
    assert "expires_at - INTERVAL '30 day'" in sql.query


def test_functions_instance() -> None:
    """Test F is a Functions instance with PostgreSQL-specific functions."""
    from vw.postgres.public import Functions

    assert isinstance(F, Functions)


def test_functions_now() -> None:
    """Test F.now() PostgreSQL function."""
    query = ref("users").select(F.now().alias("current_time"))

    sql = render(query)
    assert sql.query == "SELECT NOW() AS current_time FROM users"


def test_functions_gen_random_uuid() -> None:
    """Test F.gen_random_uuid() function."""
    query = ref("users").select(F.gen_random_uuid().alias("id"))

    sql = render(query)
    assert sql.query == "SELECT GEN_RANDOM_UUID() AS id FROM users"


def test_functions_array_agg() -> None:
    """Test F.array_agg() function."""
    query = ref("users").select(F.array_agg(col("name")).alias("names"))

    sql = render(query)
    assert sql.query == "SELECT ARRAY_AGG(name) AS names FROM users"


def test_functions_array_agg_distinct() -> None:
    """Test F.array_agg() with distinct."""
    query = ref("users").select(F.array_agg(col("name"), distinct=True).alias("names"))

    sql = render(query)
    assert sql.query == "SELECT ARRAY_AGG(DISTINCT name) AS names FROM users"


def test_functions_array_agg_order_by() -> None:
    """Test F.array_agg() with order_by."""
    query = ref("users").select(F.array_agg(col("name"), order_by=[col("name").asc()]).alias("names"))

    sql = render(query)
    assert sql.query == "SELECT ARRAY_AGG(name ORDER BY name ASC) AS names FROM users"


def test_functions_string_agg() -> None:
    """Test F.string_agg() function."""
    query = ref("users").select(F.string_agg(col("name"), lit(", ")).alias("all_names"))

    sql = render(query)
    assert sql.query == "SELECT STRING_AGG(name, ', ') AS all_names FROM users"


def test_functions_string_agg_order_by() -> None:
    """Test F.string_agg() with order_by."""
    query = ref("users").select(F.string_agg(col("name"), lit(", "), order_by=[col("name")]).alias("all_names"))

    sql = render(query)
    assert sql.query == "SELECT STRING_AGG(name, ', ' ORDER BY name) AS all_names FROM users"


def test_functions_json_build_object() -> None:
    """Test F.json_build_object() function."""
    query = ref("users").select(F.json_build_object(lit("id"), col("id"), lit("name"), col("name")).alias("user_json"))

    sql = render(query)
    assert sql.query == "SELECT JSON_BUILD_OBJECT('id', id, 'name', name) AS user_json FROM users"


def test_functions_json_agg() -> None:
    """Test F.json_agg() function."""
    query = ref("users").select(F.json_agg(col("data")).alias("json_array"))

    sql = render(query)
    assert sql.query == "SELECT JSON_AGG(data) AS json_array FROM users"


def test_functions_json_agg_order_by() -> None:
    """Test F.json_agg() with order_by."""
    query = ref("users").select(F.json_agg(col("data"), order_by=[col("created_at")]).alias("json_array"))

    sql = render(query)
    assert sql.query == "SELECT JSON_AGG(data ORDER BY created_at) AS json_array FROM users"


def test_functions_unnest() -> None:
    """Test F.unnest() function."""
    query = ref("data").select(F.unnest(col("array_col")).alias("elem"))

    sql = render(query)
    assert sql.query == "SELECT UNNEST(array_col) AS elem FROM data"


def test_functions_bit_and() -> None:
    """Test F.bit_and() function."""
    query = ref("permissions").select(F.bit_and(col("flags")).alias("combined_flags"))

    sql = render(query)
    assert sql.query == "SELECT BIT_AND(flags) AS combined_flags FROM permissions"


def test_functions_bit_or() -> None:
    """Test F.bit_or() function."""
    query = ref("permissions").select(F.bit_or(col("flags")).alias("combined_flags"))

    sql = render(query)
    assert sql.query == "SELECT BIT_OR(flags) AS combined_flags FROM permissions"


def test_functions_bool_and() -> None:
    """Test F.bool_and() function."""
    query = ref("users").select(F.bool_and(col("is_active")).alias("all_active"))

    sql = render(query)
    assert sql.query == "SELECT BOOL_AND(is_active) AS all_active FROM users"


def test_functions_bool_or() -> None:
    """Test F.bool_or() function."""
    query = ref("users").select(F.bool_or(col("needs_review")).alias("any_need_review"))

    sql = render(query)
    assert sql.query == "SELECT BOOL_OR(needs_review) AS any_need_review FROM users"


def test_functions_inherits_core_functions() -> None:
    """Test that F has core ANSI SQL functions."""
    # Test a few core functions are available
    query = ref("sales").select(F.count(col("*")).alias("total"), F.sum(col("amount")).alias("sum"))

    sql = render(query)
    assert "COUNT(*)" in sql.query
    assert "SUM(amount)" in sql.query
