"""Integration tests for DuckDB struct functions."""

from vw.duckdb import F, col, lit, ref, render


def describe_struct_pack():
    """Test STRUCT_PACK function."""

    def it_renders_single_field():
        query = ref("t").select(F.struct_pack({"name": lit("Alice")}).alias("s"))
        result = render(query)
        assert result.query == "SELECT STRUCT_PACK(name := 'Alice') AS s FROM t"
        assert result.params == {}

    def it_renders_multiple_fields():
        query = ref("t").select(F.struct_pack({"name": lit("Alice"), "age": lit(30)}).alias("person"))
        result = render(query)
        assert result.query == "SELECT STRUCT_PACK(name := 'Alice', age := 30) AS person FROM t"
        assert result.params == {}

    def it_renders_with_columns():
        query = ref("t").select(F.struct_pack({"x": col("x_val"), "y": col("y_val")}).alias("point"))
        result = render(query)
        assert result.query == "SELECT STRUCT_PACK(x := x_val, y := y_val) AS point FROM t"
        assert result.params == {}

    def it_renders_nested_struct():
        inner = F.struct_pack({"street": lit("Main St"), "zip": lit("12345")})
        query = ref("t").select(F.struct_pack({"name": lit("Alice"), "addr": inner}).alias("user"))
        result = render(query)
        assert result.query == (
            "SELECT STRUCT_PACK(name := 'Alice', addr := STRUCT_PACK(street := 'Main St', zip := '12345')) AS user FROM t"
        )
        assert result.params == {}


def describe_struct_extract():
    """Test STRUCT_EXTRACT function."""

    def it_renders_basic_usage():
        query = ref("t").select(F.struct_extract(col("address"), "city").alias("city"))
        result = render(query)
        assert result.query == "SELECT STRUCT_EXTRACT(address, 'city') AS city FROM t"
        assert result.params == {}

    def it_renders_with_numeric_field():
        query = ref("t").select(F.struct_extract(col("person"), "age").alias("age"))
        result = render(query)
        assert result.query == "SELECT STRUCT_EXTRACT(person, 'age') AS age FROM t"
        assert result.params == {}

    def it_renders_from_struct_pack():
        packed = F.struct_pack({"a": lit(1), "b": lit(2)})
        query = ref("t").select(F.struct_extract(packed, "a").alias("a_val"))
        result = render(query)
        assert result.query == "SELECT STRUCT_EXTRACT(STRUCT_PACK(a := 1, b := 2), 'a') AS a_val FROM t"
        assert result.params == {}


def describe_struct_insert():
    """Test STRUCT_INSERT function."""

    def it_renders_basic_usage():
        query = ref("t").select(F.struct_insert(col("s"), {"score": lit(100)}).alias("updated"))
        result = render(query)
        assert result.query == "SELECT STRUCT_INSERT(s, score := 100) AS updated FROM t"
        assert result.params == {}

    def it_renders_multiple_fields():
        query = ref("t").select(
            F.struct_insert(col("addr"), {"zip": lit("12345"), "country": lit("US")}).alias("full_addr")
        )
        result = render(query)
        assert result.query == "SELECT STRUCT_INSERT(addr, zip := '12345', country := 'US') AS full_addr FROM t"
        assert result.params == {}

    def it_renders_with_column_value():
        query = ref("t").select(F.struct_insert(col("person"), {"updated_name": col("new_name")}).alias("person2"))
        result = render(query)
        assert result.query == "SELECT STRUCT_INSERT(person, updated_name := new_name) AS person2 FROM t"
        assert result.params == {}


def describe_struct_keys():
    """Test STRUCT_KEYS function."""

    def it_renders_basic_usage():
        query = ref("t").select(F.struct_keys(col("s")).alias("keys"))
        result = render(query)
        assert result.query == "SELECT STRUCT_KEYS(s) AS keys FROM t"
        assert result.params == {}

    def it_renders_with_struct_pack():
        packed = F.struct_pack({"a": lit(1), "b": lit(2)})
        query = ref("t").select(F.struct_keys(packed).alias("keys"))
        result = render(query)
        assert result.query == "SELECT STRUCT_KEYS(STRUCT_PACK(a := 1, b := 2)) AS keys FROM t"
        assert result.params == {}


def describe_struct_values():
    """Test STRUCT_VALUES function."""

    def it_renders_basic_usage():
        query = ref("t").select(F.struct_values(col("s")).alias("vals"))
        result = render(query)
        assert result.query == "SELECT STRUCT_VALUES(s) AS vals FROM t"
        assert result.params == {}

    def it_renders_with_struct_pack():
        packed = F.struct_pack({"x": lit(10), "y": lit(20)})
        query = ref("t").select(F.struct_values(packed).alias("vals"))
        result = render(query)
        assert result.query == "SELECT STRUCT_VALUES(STRUCT_PACK(x := 10, y := 20)) AS vals FROM t"
        assert result.params == {}


def describe_struct_composition():
    """Test struct function composition."""

    def it_renders_struct_pack_then_extract():
        packed = F.struct_pack({"name": lit("Alice"), "age": lit(30)})
        query = ref("t").select(
            F.struct_extract(packed, "name").alias("extracted_name"),
            F.struct_extract(packed, "age").alias("extracted_age"),
        )
        result = render(query)
        assert result.query == (
            "SELECT STRUCT_EXTRACT(STRUCT_PACK(name := 'Alice', age := 30), 'name') AS extracted_name, "
            "STRUCT_EXTRACT(STRUCT_PACK(name := 'Alice', age := 30), 'age') AS extracted_age FROM t"
        )
        assert result.params == {}

    def it_renders_struct_extract_in_where():
        query = ref("t").select(col("id")).where(F.struct_extract(col("person"), "age") > lit(18))
        result = render(query)
        assert result.query == "SELECT id FROM t WHERE STRUCT_EXTRACT(person, 'age') > 18"
        assert result.params == {}
