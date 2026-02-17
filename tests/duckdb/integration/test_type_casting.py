"""Integration tests for DuckDB type casting."""

from tests.utils import sql
from vw.duckdb import T, col, lit, ref, render


def describe_cast_rendering() -> None:
    """Cast expressions should render correctly with DuckDB types."""

    def it_casts_to_tinyint() -> None:
        q = ref("users").select(col("age").cast(T.TINYINT()))
        result = render(q)
        assert result.query == sql("SELECT age::TINYINT FROM users")

    def it_casts_to_hugeint() -> None:
        q = ref("data").select(col("count").cast(T.HUGEINT()))
        result = render(q)
        assert result.query == sql("SELECT count::HUGEINT FROM data")

    def it_casts_to_unsigned_integer() -> None:
        q = ref("data").select(col("value").cast(T.UINTEGER()))
        result = render(q)
        assert result.query == sql("SELECT value::UINTEGER FROM data")

    def it_casts_to_list() -> None:
        q = ref("data").select(col("tags").cast(T.LIST(T.VARCHAR())))
        result = render(q)
        assert result.query == sql("SELECT tags::LIST<VARCHAR> FROM data")

    def it_casts_to_struct() -> None:
        q = ref("data").select(col("address").cast(T.STRUCT({"street": T.VARCHAR(), "city": T.VARCHAR()})))
        result = render(q)
        expected = sql("SELECT address::STRUCT<street: VARCHAR, city: VARCHAR> FROM data")
        assert result.query == expected

    def it_casts_to_map() -> None:
        q = ref("data").select(col("meta").cast(T.MAP(T.VARCHAR(), T.VARCHAR())))
        result = render(q)
        assert result.query == sql("SELECT meta::MAP<VARCHAR, VARCHAR> FROM data")

    def it_casts_to_fixed_array() -> None:
        q = ref("data").select(col("coords").cast(T.ARRAY(T.DOUBLE(), 3)))
        result = render(q)
        assert result.query == sql("SELECT coords::DOUBLE[3] FROM data")

    def it_casts_to_variable_array() -> None:
        q = ref("data").select(col("values").cast(T.ARRAY(T.INTEGER())))
        result = render(q)
        assert result.query == sql("SELECT values::INTEGER[] FROM data")

    def it_casts_nested_types() -> None:
        q = ref("data").select(col("matrix").cast(T.LIST(T.LIST(T.INTEGER()))))
        result = render(q)
        assert result.query == sql("SELECT matrix::LIST<LIST<INTEGER>> FROM data")


def describe_cast_in_queries() -> None:
    """Cast should work in different query contexts."""

    def it_casts_in_where_clause() -> None:
        q = ref("users").select(col("id")).where(col("age").cast(T.INTEGER()) > lit(18))
        result = render(q)
        assert result.query == sql("SELECT id FROM users WHERE age::INTEGER > 18")

    def it_casts_in_join_condition() -> None:
        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(col("u.name")).join.inner(orders, on=[col("u.id").cast(T.VARCHAR()) == col("o.user_id")])
        result = render(q)
        expected = sql("""
            SELECT u.name FROM users AS u
            INNER JOIN orders AS o ON (u.id::VARCHAR = o.user_id)
        """)
        assert result.query == expected

    def it_casts_in_order_by() -> None:
        q = ref("data").select(col("id")).order_by(col("value").cast(T.INTEGER()).desc())
        result = render(q)
        assert result.query == sql("SELECT id FROM data ORDER BY value::INTEGER DESC")

    def it_casts_in_group_by() -> None:
        from vw.duckdb import F

        q = (
            ref("data")
            .select(col("category").cast(T.VARCHAR()).alias("cat"), F.count().alias("cnt"))
            .group_by(col("category").cast(T.VARCHAR()))
        )
        result = render(q)
        expected = sql("""
            SELECT category::VARCHAR AS cat, COUNT(*) AS cnt
            FROM data
            GROUP BY category::VARCHAR
        """)
        assert result.query == expected

    def it_casts_multiple_columns() -> None:
        q = ref("data").select(
            col("id").cast(T.BIGINT()), col("age").cast(T.TINYINT()), col("tags").cast(T.LIST(T.VARCHAR()))
        )
        result = render(q)
        expected = sql("""
            SELECT id::BIGINT, age::TINYINT, tags::LIST<VARCHAR>
            FROM data
        """)
        assert result.query == expected


def describe_complex_nested_types() -> None:
    """Complex nested type structures should render correctly."""

    def it_casts_struct_with_nested_list() -> None:
        q = ref("data").select(col("user").cast(T.STRUCT({"id": T.INTEGER(), "tags": T.LIST(T.VARCHAR())})))
        result = render(q)
        expected = sql("""
            SELECT user::STRUCT<id: INTEGER, tags: LIST<VARCHAR>>
            FROM data
        """)
        assert result.query == expected

    def it_casts_list_of_structs() -> None:
        q = ref("data").select(col("users").cast(T.LIST(T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR()}))))
        result = render(q)
        expected = sql("""
            SELECT users::LIST<STRUCT<id: INTEGER, name: VARCHAR>>
            FROM data
        """)
        assert result.query == expected

    def it_casts_map_with_struct_values() -> None:
        q = ref("data").select(
            col("metadata").cast(T.MAP(T.VARCHAR(), T.STRUCT({"count": T.INTEGER(), "total": T.DOUBLE()})))
        )
        result = render(q)
        expected = sql("""
            SELECT metadata::MAP<VARCHAR, STRUCT<count: INTEGER, total: DOUBLE>>
            FROM data
        """)
        assert result.query == expected

    def it_casts_deeply_nested_structure() -> None:
        q = ref("data").select(
            col("complex").cast(T.STRUCT({"name": T.VARCHAR(), "data": T.MAP(T.VARCHAR(), T.LIST(T.INTEGER()))}))
        )
        result = render(q)
        expected = sql("""
            SELECT complex::STRUCT<name: VARCHAR, data: MAP<VARCHAR, LIST<INTEGER>>>
            FROM data
        """)
        assert result.query == expected


def describe_t_shorthand_usage() -> None:
    """T shorthand should work correctly."""

    def it_imports_t_from_duckdb() -> None:
        from vw.duckdb import T as TypeShorthand

        assert TypeShorthand.INTEGER() == "INTEGER"

    def it_uses_t_for_simple_types() -> None:
        q = ref("users").select(col("age").cast(T.TINYINT()))
        result = render(q)
        assert result.query == sql("SELECT age::TINYINT FROM users")

    def it_uses_t_for_complex_types() -> None:
        q = ref("data").select(col("info").cast(T.STRUCT({"id": T.INTEGER(), "tags": T.LIST(T.VARCHAR())})))
        result = render(q)
        expected = sql("SELECT info::STRUCT<id: INTEGER, tags: LIST<VARCHAR>> FROM data")
        assert result.query == expected
