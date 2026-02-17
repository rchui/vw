"""Unit tests for DuckDB type descriptor functions."""

from vw.duckdb import types as T


def describe_core_type_reexports() -> None:
    """Core ANSI types should be accessible from vw.duckdb.types."""

    def it_exports_integer() -> None:
        assert T.INTEGER() == "INTEGER"

    def it_exports_bigint() -> None:
        assert T.BIGINT() == "BIGINT"

    def it_exports_smallint() -> None:
        assert T.SMALLINT() == "SMALLINT"

    def it_exports_real() -> None:
        assert T.REAL() == "REAL"

    def it_exports_double_precision() -> None:
        assert T.DOUBLE_PRECISION() == "DOUBLE PRECISION"

    def it_exports_text() -> None:
        assert T.TEXT() == "TEXT"

    def it_exports_varchar() -> None:
        assert T.VARCHAR() == "VARCHAR"

    def it_exports_varchar_with_length() -> None:
        assert T.VARCHAR(255) == "VARCHAR(255)"

    def it_exports_char() -> None:
        assert T.CHAR() == "CHAR"

    def it_exports_char_with_length() -> None:
        assert T.CHAR(10) == "CHAR(10)"

    def it_exports_boolean() -> None:
        assert T.BOOLEAN() == "BOOLEAN"

    def it_exports_date() -> None:
        assert T.DATE() == "DATE"

    def it_exports_time() -> None:
        assert T.TIME() == "TIME"

    def it_exports_timestamp() -> None:
        assert T.TIMESTAMP() == "TIMESTAMP"

    def it_exports_json() -> None:
        assert T.JSON() == "JSON"

    def it_exports_uuid() -> None:
        assert T.UUID() == "UUID"

    def it_exports_bytea() -> None:
        assert T.BYTEA() == "BYTEA"

    def it_exports_numeric() -> None:
        assert T.NUMERIC() == "NUMERIC"

    def it_exports_numeric_with_precision() -> None:
        assert T.NUMERIC(10) == "NUMERIC(10)"

    def it_exports_numeric_with_precision_and_scale() -> None:
        assert T.NUMERIC(10, 2) == "NUMERIC(10,2)"

    def it_exports_decimal() -> None:
        assert T.DECIMAL() == "DECIMAL"

    def it_exports_decimal_with_precision() -> None:
        assert T.DECIMAL(10) == "DECIMAL(10)"

    def it_exports_decimal_with_precision_and_scale() -> None:
        assert T.DECIMAL(10, 2) == "DECIMAL(10,2)"


def describe_duckdb_integer_variants() -> None:
    """DuckDB-specific integer types."""

    def it_supports_tinyint() -> None:
        assert T.TINYINT() == "TINYINT"

    def it_supports_utinyint() -> None:
        assert T.UTINYINT() == "UTINYINT"

    def it_supports_usmallint() -> None:
        assert T.USMALLINT() == "USMALLINT"

    def it_supports_uinteger() -> None:
        assert T.UINTEGER() == "UINTEGER"

    def it_supports_ubigint() -> None:
        assert T.UBIGINT() == "UBIGINT"

    def it_supports_hugeint() -> None:
        assert T.HUGEINT() == "HUGEINT"

    def it_supports_uhugeint() -> None:
        assert T.UHUGEINT() == "UHUGEINT"


def describe_duckdb_simple_types() -> None:
    """DuckDB-specific simple types."""

    def it_supports_float() -> None:
        assert T.FLOAT() == "FLOAT"

    def it_supports_double() -> None:
        assert T.DOUBLE() == "DOUBLE"

    def it_supports_blob() -> None:
        assert T.BLOB() == "BLOB"

    def it_supports_bit() -> None:
        assert T.BIT() == "BIT"

    def it_supports_interval() -> None:
        assert T.INTERVAL() == "INTERVAL"


def describe_list_type() -> None:
    """LIST type for variable-length arrays."""

    def it_creates_simple_list() -> None:
        assert T.LIST(T.INTEGER()) == "LIST<INTEGER>"

    def it_creates_list_of_varchar() -> None:
        assert T.LIST(T.VARCHAR()) == "LIST<VARCHAR>"

    def it_creates_list_of_text() -> None:
        assert T.LIST(T.TEXT()) == "LIST<TEXT>"

    def it_creates_nested_list() -> None:
        assert T.LIST(T.LIST(T.INTEGER())) == "LIST<LIST<INTEGER>>"

    def it_creates_deeply_nested_list() -> None:
        result = T.LIST(T.LIST(T.LIST(T.VARCHAR())))
        assert result == "LIST<LIST<LIST<VARCHAR>>>"

    def it_creates_list_of_struct() -> None:
        result = T.LIST(T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR()}))
        assert result == "LIST<STRUCT<id: INTEGER, name: VARCHAR>>"


def describe_struct_type() -> None:
    """STRUCT type for named record types."""

    def it_creates_simple_struct() -> None:
        assert T.STRUCT({"id": T.INTEGER()}) == "STRUCT<id: INTEGER>"

    def it_creates_multi_field_struct() -> None:
        result = T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR()})
        assert result == "STRUCT<id: INTEGER, name: VARCHAR>"

    def it_creates_three_field_struct() -> None:
        result = T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR(), "active": T.BOOLEAN()})
        assert result == "STRUCT<id: INTEGER, name: VARCHAR, active: BOOLEAN>"

    def it_creates_struct_with_list_field() -> None:
        result = T.STRUCT({"id": T.INTEGER(), "tags": T.LIST(T.VARCHAR())})
        expected = "STRUCT<id: INTEGER, tags: LIST<VARCHAR>>"
        assert result == expected

    def it_creates_struct_with_map_field() -> None:
        result = T.STRUCT({"id": T.INTEGER(), "metadata": T.MAP(T.VARCHAR(), T.VARCHAR())})
        expected = "STRUCT<id: INTEGER, metadata: MAP<VARCHAR, VARCHAR>>"
        assert result == expected

    def it_creates_struct_with_nested_types() -> None:
        result = T.STRUCT({"id": T.INTEGER(), "tags": T.LIST(T.VARCHAR()), "metadata": T.MAP(T.VARCHAR(), T.VARCHAR())})
        expected = "STRUCT<id: INTEGER, tags: LIST<VARCHAR>, metadata: MAP<VARCHAR, VARCHAR>>"
        assert result == expected

    def it_creates_nested_struct() -> None:
        result = T.STRUCT({"user": T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR()}), "active": T.BOOLEAN()})
        expected = "STRUCT<user: STRUCT<id: INTEGER, name: VARCHAR>, active: BOOLEAN>"
        assert result == expected


def describe_map_type() -> None:
    """MAP type for key-value pairs."""

    def it_creates_simple_map() -> None:
        assert T.MAP(T.VARCHAR(), T.INTEGER()) == "MAP<VARCHAR, INTEGER>"

    def it_creates_string_to_string_map() -> None:
        assert T.MAP(T.VARCHAR(), T.VARCHAR()) == "MAP<VARCHAR, VARCHAR>"

    def it_creates_integer_key_map() -> None:
        assert T.MAP(T.INTEGER(), T.TEXT()) == "MAP<INTEGER, TEXT>"

    def it_creates_map_with_list_values() -> None:
        result = T.MAP(T.VARCHAR(), T.LIST(T.INTEGER()))
        assert result == "MAP<VARCHAR, LIST<INTEGER>>"

    def it_creates_map_with_struct_values() -> None:
        result = T.MAP(T.VARCHAR(), T.STRUCT({"id": T.INTEGER(), "name": T.VARCHAR()}))
        expected = "MAP<VARCHAR, STRUCT<id: INTEGER, name: VARCHAR>>"
        assert result == expected

    def it_creates_nested_map() -> None:
        result = T.MAP(T.VARCHAR(), T.MAP(T.VARCHAR(), T.INTEGER()))
        assert result == "MAP<VARCHAR, MAP<VARCHAR, INTEGER>>"


def describe_array_type() -> None:
    """ARRAY type for fixed-length arrays."""

    def it_creates_variable_length_array() -> None:
        assert T.ARRAY(T.INTEGER()) == "INTEGER[]"

    def it_creates_fixed_length_array() -> None:
        assert T.ARRAY(T.DOUBLE(), 3) == "DOUBLE[3]"

    def it_creates_varchar_array() -> None:
        assert T.ARRAY(T.VARCHAR()) == "VARCHAR[]"

    def it_creates_fixed_size_varchar_array() -> None:
        assert T.ARRAY(T.VARCHAR(), 10) == "VARCHAR[10]"

    def it_creates_large_fixed_array() -> None:
        assert T.ARRAY(T.INTEGER(), 1000) == "INTEGER[1000]"


def describe_type_composition() -> None:
    """Complex nested type compositions."""

    def it_composes_list_of_maps() -> None:
        result = T.LIST(T.MAP(T.VARCHAR(), T.INTEGER()))
        assert result == "LIST<MAP<VARCHAR, INTEGER>>"

    def it_composes_struct_with_all_types() -> None:
        result = T.STRUCT(
            {
                "id": T.INTEGER(),
                "tags": T.LIST(T.VARCHAR()),
                "metadata": T.MAP(T.VARCHAR(), T.VARCHAR()),
                "scores": T.ARRAY(T.DOUBLE(), 5),
            }
        )
        expected = "STRUCT<id: INTEGER, tags: LIST<VARCHAR>, metadata: MAP<VARCHAR, VARCHAR>, scores: DOUBLE[5]>"
        assert result == expected

    def it_composes_map_of_structs() -> None:
        result = T.MAP(T.VARCHAR(), T.STRUCT({"count": T.INTEGER(), "total": T.DOUBLE()}))
        expected = "MAP<VARCHAR, STRUCT<count: INTEGER, total: DOUBLE>>"
        assert result == expected

    def it_composes_deeply_nested_structure() -> None:
        result = T.LIST(T.STRUCT({"name": T.VARCHAR(), "data": T.MAP(T.VARCHAR(), T.LIST(T.INTEGER()))}))
        expected = "LIST<STRUCT<name: VARCHAR, data: MAP<VARCHAR, LIST<INTEGER>>>>"
        assert result == expected

    def it_uses_integer_variants_in_composition() -> None:
        result = T.STRUCT({"tiny": T.TINYINT(), "huge": T.HUGEINT(), "unsigned": T.UINTEGER()})
        expected = "STRUCT<tiny: TINYINT, huge: HUGEINT, unsigned: UINTEGER>"
        assert result == expected
