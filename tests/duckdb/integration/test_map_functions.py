"""Integration tests for DuckDB map functions."""

from vw.duckdb import F, col, lit, ref, render


def describe_map_functions():
    """Test DuckDB map construction and access functions."""

    def describe_map():
        """Test MAP() function."""

        def it_renders_map_from_two_lists():
            query = ref("t").select(
                F.map(F.list_value(lit("a"), lit("b")), F.list_value(lit(1), lit(2))).alias("m")
            )
            result = render(query)
            assert result.query == "SELECT MAP(LIST_VALUE('a', 'b'), LIST_VALUE(1, 2)) AS m FROM t"
            assert result.params == {}

        def it_renders_map_from_columns():
            query = ref("t").select(F.map(col("keys"), col("vals")).alias("m"))
            result = render(query)
            assert result.query == "SELECT MAP(keys, vals) AS m FROM t"
            assert result.params == {}

    def describe_map_extract():
        """Test MAP_EXTRACT() function."""

        def it_renders_map_extract_with_literal_key():
            query = ref("t").select(F.map_extract(col("config"), lit("timeout")).alias("timeout"))
            result = render(query)
            assert result.query == "SELECT MAP_EXTRACT(config, 'timeout') AS timeout FROM t"
            assert result.params == {}

        def it_renders_map_extract_with_column_key():
            query = ref("t").select(F.map_extract(col("scores"), col("user_id")).alias("score"))
            result = render(query)
            assert result.query == "SELECT MAP_EXTRACT(scores, user_id) AS score FROM t"
            assert result.params == {}

    def describe_map_keys():
        """Test MAP_KEYS() function."""

        def it_renders_map_keys():
            query = ref("t").select(F.map_keys(col("config")).alias("keys"))
            result = render(query)
            assert result.query == "SELECT MAP_KEYS(config) AS keys FROM t"
            assert result.params == {}

    def describe_map_values():
        """Test MAP_VALUES() function."""

        def it_renders_map_values():
            query = ref("t").select(F.map_values(col("config")).alias("vals"))
            result = render(query)
            assert result.query == "SELECT MAP_VALUES(config) AS vals FROM t"
            assert result.params == {}

    def describe_map_from_entries():
        """Test MAP_FROM_ENTRIES() function."""

        def it_renders_map_from_entries():
            query = ref("t").select(F.map_from_entries(col("kv_pairs")).alias("m"))
            result = render(query)
            assert result.query == "SELECT MAP_FROM_ENTRIES(kv_pairs) AS m FROM t"
            assert result.params == {}

    def describe_map_concat():
        """Test MAP_CONCAT() function."""

        def it_renders_two_maps():
            query = ref("t").select(F.map_concat(col("map1"), col("map2")).alias("merged"))
            result = render(query)
            assert result.query == "SELECT MAP_CONCAT(map1, map2) AS merged FROM t"
            assert result.params == {}

        def it_renders_three_maps():
            query = ref("t").select(F.map_concat(col("a"), col("b"), col("c")).alias("merged"))
            result = render(query)
            assert result.query == "SELECT MAP_CONCAT(a, b, c) AS merged FROM t"
            assert result.params == {}

    def describe_cardinality():
        """Test CARDINALITY() function."""

        def it_renders_cardinality_on_map():
            query = ref("t").select(F.cardinality(col("config")).alias("size"))
            result = render(query)
            assert result.query == "SELECT CARDINALITY(config) AS size FROM t"
            assert result.params == {}

        def it_renders_cardinality_on_list():
            query = ref("t").select(F.cardinality(col("tags")).alias("tag_count"))
            result = render(query)
            assert result.query == "SELECT CARDINALITY(tags) AS tag_count FROM t"
            assert result.params == {}

        def it_renders_cardinality_in_where():
            query = ref("t").select(col("id")).where(F.cardinality(col("tags")) > lit(0))
            result = render(query)
            assert result.query == "SELECT id FROM t WHERE CARDINALITY(tags) > 0"
            assert result.params == {}
