"""Integration tests for DuckDB JSON functions."""

from vw.duckdb import F, col, lit, ref, render


def describe_json_functions():
    """Test DuckDB JSON extraction and manipulation functions."""

    def describe_json_extract():
        """Test JSON_EXTRACT() function."""

        def it_renders_basic_path():
            query = ref("t").select(F.json_extract(col("data"), lit("$.name")).alias("name"))
            result = render(query)
            assert result.query == "SELECT JSON_EXTRACT(data, '$.name') AS name FROM t"
            assert result.params == {}

        def it_renders_nested_path():
            query = ref("t").select(F.json_extract(col("event"), lit("$.payload.user_id")).alias("uid"))
            result = render(query)
            assert result.query == "SELECT JSON_EXTRACT(event, '$.payload.user_id') AS uid FROM t"
            assert result.params == {}

    def describe_json_extract_string():
        """Test JSON_EXTRACT_STRING() function."""

        def it_renders_extract_as_varchar():
            query = ref("t").select(F.json_extract_string(col("data"), lit("$.name")).alias("name"))
            result = render(query)
            assert result.query == "SELECT JSON_EXTRACT_STRING(data, '$.name') AS name FROM t"
            assert result.params == {}

    def describe_json_array_length():
        """Test JSON_ARRAY_LENGTH() function."""

        def it_renders_without_path():
            query = ref("t").select(F.json_array_length(col("items")).alias("count"))
            result = render(query)
            assert result.query == "SELECT JSON_ARRAY_LENGTH(items) AS count FROM t"
            assert result.params == {}

        def it_renders_with_path():
            query = ref("t").select(F.json_array_length(col("data"), lit("$.items")).alias("count"))
            result = render(query)
            assert result.query == "SELECT JSON_ARRAY_LENGTH(data, '$.items') AS count FROM t"
            assert result.params == {}

    def describe_json_type():
        """Test JSON_TYPE() function."""

        def it_renders_without_path():
            query = ref("t").select(F.json_type(col("data")).alias("jtype"))
            result = render(query)
            assert result.query == "SELECT JSON_TYPE(data) AS jtype FROM t"
            assert result.params == {}

        def it_renders_with_path():
            query = ref("t").select(F.json_type(col("data"), lit("$.value")).alias("jtype"))
            result = render(query)
            assert result.query == "SELECT JSON_TYPE(data, '$.value') AS jtype FROM t"
            assert result.params == {}

    def describe_json_valid():
        """Test JSON_VALID() function."""

        def it_renders_json_valid():
            query = ref("t").select(F.json_valid(col("raw_json")).alias("is_valid"))
            result = render(query)
            assert result.query == "SELECT JSON_VALID(raw_json) AS is_valid FROM t"
            assert result.params == {}

        def it_renders_in_where_clause():
            query = ref("t").select(col("id")).where(F.json_valid(col("payload")))
            result = render(query)
            assert result.query == "SELECT id FROM t WHERE JSON_VALID(payload)"
            assert result.params == {}

    def describe_json_keys():
        """Test JSON_KEYS() function."""

        def it_renders_without_path():
            query = ref("t").select(F.json_keys(col("data")).alias("keys"))
            result = render(query)
            assert result.query == "SELECT JSON_KEYS(data) AS keys FROM t"
            assert result.params == {}

        def it_renders_with_path():
            query = ref("t").select(F.json_keys(col("data"), lit("$.nested")).alias("keys"))
            result = render(query)
            assert result.query == "SELECT JSON_KEYS(data, '$.nested') AS keys FROM t"
            assert result.params == {}

    def describe_to_json():
        """Test TO_JSON() function."""

        def it_renders_column_to_json():
            query = ref("t").select(F.to_json(col("data")).alias("json_data"))
            result = render(query)
            assert result.query == "SELECT TO_JSON(data) AS json_data FROM t"
            assert result.params == {}

        def it_renders_struct_to_json():
            query = ref("t").select(
                F.to_json(F.struct_pack({"name": lit("Alice"), "age": lit(30)})).alias("json_obj")
            )
            result = render(query)
            assert result.query == "SELECT TO_JSON(STRUCT_PACK(name := 'Alice', age := 30)) AS json_obj FROM t"
            assert result.params == {}

    def describe_json_object():
        """Test JSON_OBJECT() function."""

        def it_renders_key_value_pairs():
            query = ref("t").select(
                F.json_object(lit("name"), col("name"), lit("age"), col("age")).alias("obj")
            )
            result = render(query)
            assert result.query == "SELECT JSON_OBJECT('name', name, 'age', age) AS obj FROM t"
            assert result.params == {}

    def describe_json_array():
        """Test JSON_ARRAY() function."""

        def it_renders_literal_elements():
            query = ref("t").select(F.json_array(lit(1), lit(2), lit(3)).alias("arr"))
            result = render(query)
            assert result.query == "SELECT JSON_ARRAY(1, 2, 3) AS arr FROM t"
            assert result.params == {}

        def it_renders_column_elements():
            query = ref("t").select(F.json_array(col("a"), col("b")).alias("arr"))
            result = render(query)
            assert result.query == "SELECT JSON_ARRAY(a, b) AS arr FROM t"
            assert result.params == {}
