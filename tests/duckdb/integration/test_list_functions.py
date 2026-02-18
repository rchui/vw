"""Integration tests for DuckDB list functions."""

from vw.duckdb import F, col, lit, ref, render


def describe_list_construction():
    """Test list construction functions."""

    def describe_list_value():
        """Test LIST_VALUE function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_value(lit(1), lit(2), lit(3)).alias("nums"))
            result = render(query)
            assert result.query == "SELECT LIST_VALUE(1, 2, 3) AS nums FROM t"
            assert result.params == {}

        def it_renders_with_columns():
            query = ref("t").select(F.list_value(col("a"), col("b"), col("c")).alias("combined"))
            result = render(query)
            assert result.query == "SELECT LIST_VALUE(a, b, c) AS combined FROM t"
            assert result.params == {}

        def it_renders_empty_list():
            query = ref("t").select(F.list_value().alias("empty"))
            result = render(query)
            assert result.query == "SELECT LIST_VALUE() AS empty FROM t"
            assert result.params == {}

        def it_renders_with_mixed_types():
            query = ref("t").select(F.list_value(col("id"), lit("text"), lit(42)).alias("mixed"))
            result = render(query)
            assert result.query == "SELECT LIST_VALUE(id, 'text', 42) AS mixed FROM t"
            assert result.params == {}

    def describe_list_agg():
        """Test LIST_AGG function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_agg(col("name")).alias("names"))
            result = render(query)
            assert result.query == "SELECT LIST_AGG(name) AS names FROM t"
            assert result.params == {}

        def it_renders_with_distinct():
            query = ref("t").select(F.list_agg(col("tag"), distinct=True).alias("unique_tags"))
            result = render(query)
            assert result.query == "SELECT LIST_AGG(DISTINCT tag) AS unique_tags FROM t"
            assert result.params == {}

        def it_renders_with_order_by():
            query = ref("t").select(F.list_agg(col("name"), order_by=[col("name").asc()]).alias("sorted_names"))
            result = render(query)
            assert result.query == "SELECT LIST_AGG(name ORDER BY name ASC) AS sorted_names FROM t"
            assert result.params == {}

        def it_renders_with_distinct_and_order_by():
            query = ref("t").select(
                F.list_agg(col("tag"), distinct=True, order_by=[col("tag").desc()]).alias("unique_sorted_tags")
            )
            result = render(query)
            assert result.query == "SELECT LIST_AGG(DISTINCT tag ORDER BY tag DESC) AS unique_sorted_tags FROM t"
            assert result.params == {}

        def it_works_with_group_by():
            query = (
                ref("orders")
                .select(col("user_id"), F.list_agg(col("product")).alias("products"))
                .group_by(col("user_id"))
            )
            result = render(query)
            assert result.query == "SELECT user_id, LIST_AGG(product) AS products FROM orders GROUP BY user_id"
            assert result.params == {}


def describe_list_access():
    """Test list access functions."""

    def describe_list_extract():
        """Test LIST_EXTRACT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_extract(col("tags"), lit(1)).alias("first_tag"))
            result = render(query)
            assert result.query == "SELECT LIST_EXTRACT(tags, 1) AS first_tag FROM t"
            assert result.params == {}

        def it_renders_with_negative_index():
            query = ref("t").select(F.list_extract(col("items"), lit(-1)).alias("last_item"))
            result = render(query)
            assert result.query == "SELECT LIST_EXTRACT(items, -1) AS last_item FROM t"
            assert result.params == {}

        def it_renders_with_column_index():
            query = ref("t").select(F.list_extract(col("data"), col("idx")).alias("element"))
            result = render(query)
            assert result.query == "SELECT LIST_EXTRACT(data, idx) AS element FROM t"
            assert result.params == {}

        def it_works_in_where_clause():
            query = ref("t").select(col("*")).where(F.list_extract(col("tags"), lit(1)) == lit("important"))
            result = render(query)
            assert result.query == "SELECT * FROM t WHERE LIST_EXTRACT(tags, 1) = 'important'"
            assert result.params == {}

    def describe_list_slice():
        """Test LIST_SLICE function."""

        def it_renders_basic_slice():
            query = ref("t").select(F.list_slice(col("items"), lit(1), lit(3)).alias("slice"))
            result = render(query)
            assert result.query == "SELECT LIST_SLICE(items, 1, 3) AS slice FROM t"
            assert result.params == {}

        def it_renders_with_negative_indices():
            query = ref("t").select(F.list_slice(col("data"), lit(-5), lit(-1)).alias("last_five"))
            result = render(query)
            assert result.query == "SELECT LIST_SLICE(data, -5, -1) AS last_five FROM t"
            assert result.params == {}

        def it_renders_with_step():
            query = ref("t").select(F.list_slice(col("nums"), lit(1), lit(10), lit(2)).alias("evens"))
            result = render(query)
            assert result.query == "SELECT LIST_SLICE(nums, 1, 10, 2) AS evens FROM t"
            assert result.params == {}

        def it_renders_with_column_bounds():
            query = ref("t").select(F.list_slice(col("items"), col("start"), col("end")).alias("sub_items"))
            result = render(query)
            assert result.query == "SELECT LIST_SLICE(items, start, end) AS sub_items FROM t"
            assert result.params == {}


def describe_list_membership():
    """Test list membership functions."""

    def describe_list_contains():
        """Test LIST_CONTAINS function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_contains(col("tags"), lit("important")).alias("is_important"))
            result = render(query)
            assert result.query == "SELECT LIST_CONTAINS(tags, 'important') AS is_important FROM t"
            assert result.params == {}

        def it_renders_with_column_element():
            query = ref("t").select(F.list_contains(col("allowed_ids"), col("user_id")).alias("is_allowed"))
            result = render(query)
            assert result.query == "SELECT LIST_CONTAINS(allowed_ids, user_id) AS is_allowed FROM t"
            assert result.params == {}

        def it_works_in_where_clause():
            query = ref("posts").select(col("*")).where(F.list_contains(col("tags"), lit("python")))
            result = render(query)
            assert result.query == "SELECT * FROM posts WHERE LIST_CONTAINS(tags, 'python')"
            assert result.params == {}

        def it_works_in_having_clause():
            query = (
                ref("t")
                .select(col("category"), F.count().alias("cnt"))
                .group_by(col("category"))
                .having(F.list_contains(col("allowed"), col("category")))
            )
            result = render(query)
            assert result.query == (
                "SELECT category, COUNT(*) AS cnt FROM t GROUP BY category HAVING LIST_CONTAINS(allowed, category)"
            )
            assert result.params == {}


def describe_list_modification():
    """Test list modification functions."""

    def describe_list_concat():
        """Test LIST_CONCAT function."""

        def it_renders_with_two_lists():
            query = ref("t").select(F.list_concat(col("list1"), col("list2")).alias("combined"))
            result = render(query)
            assert result.query == "SELECT LIST_CONCAT(list1, list2) AS combined FROM t"
            assert result.params == {}

        def it_renders_with_multiple_lists():
            query = ref("t").select(F.list_concat(col("a"), col("b"), col("c")).alias("all"))
            result = render(query)
            assert result.query == "SELECT LIST_CONCAT(a, b, c) AS all FROM t"
            assert result.params == {}

        def it_renders_with_list_value():
            query = ref("t").select(F.list_concat(col("existing"), F.list_value(lit(1), lit(2))).alias("extended"))
            result = render(query)
            assert result.query == "SELECT LIST_CONCAT(existing, LIST_VALUE(1, 2)) AS extended FROM t"
            assert result.params == {}

    def describe_list_append():
        """Test LIST_APPEND function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_append(col("tags"), lit("new")).alias("updated_tags"))
            result = render(query)
            assert result.query == "SELECT LIST_APPEND(tags, 'new') AS updated_tags FROM t"
            assert result.params == {}

        def it_renders_with_column_element():
            query = ref("t").select(F.list_append(col("items"), col("next_item")).alias("all_items"))
            result = render(query)
            assert result.query == "SELECT LIST_APPEND(items, next_item) AS all_items FROM t"
            assert result.params == {}

        def it_chains_multiple_appends():
            query = ref("t").select(F.list_append(F.list_append(col("base"), lit("a")), lit("b")).alias("extended"))
            result = render(query)
            assert result.query == "SELECT LIST_APPEND(LIST_APPEND(base, 'a'), 'b') AS extended FROM t"
            assert result.params == {}

    def describe_list_prepend():
        """Test LIST_PREPEND function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_prepend(lit("first"), col("tags")).alias("updated_tags"))
            result = render(query)
            assert result.query == "SELECT LIST_PREPEND('first', tags) AS updated_tags FROM t"
            assert result.params == {}

        def it_renders_with_column_element():
            query = ref("t").select(F.list_prepend(col("new_item"), col("items")).alias("all_items"))
            result = render(query)
            assert result.query == "SELECT LIST_PREPEND(new_item, items) AS all_items FROM t"
            assert result.params == {}

        def it_chains_multiple_prepends():
            query = ref("t").select(F.list_prepend(lit("a"), F.list_prepend(lit("b"), col("base"))).alias("prefixed"))
            result = render(query)
            assert result.query == "SELECT LIST_PREPEND('a', LIST_PREPEND('b', base)) AS prefixed FROM t"
            assert result.params == {}


def describe_complex_list_operations():
    """Test complex combinations of list functions."""

    def it_combines_list_value_and_list_contains():
        query = ref("t").select(col("*")).where(F.list_contains(F.list_value(lit("a"), lit("b"), lit("c")), col("x")))
        result = render(query)
        assert result.query == "SELECT * FROM t WHERE LIST_CONTAINS(LIST_VALUE('a', 'b', 'c'), x)"
        assert result.params == {}

    def it_combines_list_extract_and_list_slice():
        query = ref("t").select(F.list_extract(F.list_slice(col("items"), lit(1), lit(5)), lit(1)).alias("first"))
        result = render(query)
        assert result.query == "SELECT LIST_EXTRACT(LIST_SLICE(items, 1, 5), 1) AS first FROM t"
        assert result.params == {}

    def it_combines_list_agg_with_group_by_and_having():
        query = (
            ref("orders")
            .select(col("user_id"), F.list_agg(col("product"), order_by=[col("date").asc()]).alias("products"))
            .group_by(col("user_id"))
            .having(F.count() > lit(5))
        )
        result = render(query)
        assert (
            result.query == "SELECT user_id, LIST_AGG(product ORDER BY date ASC) AS products FROM orders "
            "GROUP BY user_id HAVING COUNT(*) > 5"
        )
        assert result.params == {}

    def it_uses_list_functions_in_cte():
        from vw.duckdb import cte

        tagged = cte("tagged", ref("posts").select(col("id"), F.list_value(col("tag1"), col("tag2")).alias("all_tags")))
        query = tagged.select(col("id")).where(F.list_contains(col("all_tags"), lit("python")))
        result = render(query)
        assert result.query == (
            "WITH tagged AS (SELECT id, LIST_VALUE(tag1, tag2) AS all_tags FROM posts) "
            "SELECT id FROM tagged WHERE LIST_CONTAINS(all_tags, 'python')"
        )
        assert result.params == {}

    def it_uses_list_functions_in_join():
        users = ref("users").alias("u")
        posts = ref("posts").alias("p")
        query = users.join.inner(posts, on=[F.list_contains(col("p.allowed_user_ids"), col("u.id"))]).select(
            col("u.name"), col("p.title")
        )
        result = render(query)
        assert result.query == (
            "SELECT u.name, p.title FROM users AS u INNER JOIN posts AS p ON (LIST_CONTAINS(p.allowed_user_ids, u.id))"
        )
        assert result.params == {}


def describe_list_transformation():
    """Test list transformation functions."""

    def describe_list_sort():
        """Test LIST_SORT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_sort(col("numbers")).alias("sorted"))
            result = render(query)
            assert result.query == "SELECT LIST_SORT(numbers) AS sorted FROM t"
            assert result.params == {}

        def it_works_in_where_clause():
            query = ref("t").select(col("*")).where(F.list_sort(col("tags")) == F.list_value(lit("a"), lit("b")))
            result = render(query)
            assert result.query == "SELECT * FROM t WHERE LIST_SORT(tags) = LIST_VALUE('a', 'b')"
            assert result.params == {}

    def describe_list_reverse():
        """Test LIST_REVERSE function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_reverse(col("items")).alias("reversed"))
            result = render(query)
            assert result.query == "SELECT LIST_REVERSE(items) AS reversed FROM t"
            assert result.params == {}

        def it_chains_with_other_functions():
            query = ref("t").select(F.list_reverse(F.list_sort(col("nums"))).alias("desc_sorted"))
            result = render(query)
            assert result.query == "SELECT LIST_REVERSE(LIST_SORT(nums)) AS desc_sorted FROM t"
            assert result.params == {}

    def describe_list_distinct():
        """Test LIST_DISTINCT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_distinct(col("tags")).alias("unique_tags"))
            result = render(query)
            assert result.query == "SELECT LIST_DISTINCT(tags) AS unique_tags FROM t"
            assert result.params == {}

        def it_works_with_list_concat():
            query = ref("t").select(F.list_distinct(F.list_concat(col("list1"), col("list2"))).alias("combined_unique"))
            result = render(query)
            assert result.query == "SELECT LIST_DISTINCT(LIST_CONCAT(list1, list2)) AS combined_unique FROM t"
            assert result.params == {}

    def describe_list_has_any():
        """Test LIST_HAS_ANY function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_has_any(col("user_tags"), col("required_tags")).alias("has_match"))
            result = render(query)
            assert result.query == "SELECT LIST_HAS_ANY(user_tags, required_tags) AS has_match FROM t"
            assert result.params == {}

        def it_works_with_list_value():
            query = ref("t").select(
                F.list_has_any(col("permissions"), F.list_value(lit("read"), lit("write"))).alias("can_access")
            )
            result = render(query)
            assert result.query == (
                "SELECT LIST_HAS_ANY(permissions, LIST_VALUE('read', 'write')) AS can_access FROM t"
            )
            assert result.params == {}

        def it_works_in_where_clause():
            query = (
                ref("users")
                .select(col("*"))
                .where(F.list_has_any(col("roles"), F.list_value(lit("admin"), lit("moderator"))))
            )
            result = render(query)
            assert result.query == ("SELECT * FROM users WHERE LIST_HAS_ANY(roles, LIST_VALUE('admin', 'moderator'))")
            assert result.params == {}

    def describe_list_has_all():
        """Test LIST_HAS_ALL function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_has_all(col("permissions"), col("required_perms")).alias("has_all"))
            result = render(query)
            assert result.query == "SELECT LIST_HAS_ALL(permissions, required_perms) AS has_all FROM t"
            assert result.params == {}

        def it_works_with_list_value():
            query = ref("t").select(
                F.list_has_all(col("tags"), F.list_value(lit("python"), lit("tutorial"))).alias("is_python_tutorial")
            )
            result = render(query)
            assert result.query == (
                "SELECT LIST_HAS_ALL(tags, LIST_VALUE('python', 'tutorial')) AS is_python_tutorial FROM t"
            )
            assert result.params == {}

        def it_works_in_having_clause():
            query = (
                ref("posts")
                .select(col("category"), F.count().alias("cnt"))
                .group_by(col("category"))
                .having(F.list_has_all(col("required_tags"), F.list_value(lit("verified"))))
            )
            result = render(query)
            assert result.query == (
                "SELECT category, COUNT(*) AS cnt FROM posts GROUP BY category "
                "HAVING LIST_HAS_ALL(required_tags, LIST_VALUE('verified'))"
            )
            assert result.params == {}

    def describe_flatten():
        """Test FLATTEN function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.flatten(col("nested_lists")).alias("flat"))
            result = render(query)
            assert result.query == "SELECT FLATTEN(nested_lists) AS flat FROM t"
            assert result.params == {}

        def it_works_with_nested_lists():
            query = ref("t").select(
                F.flatten(F.list_value(F.list_value(lit(1), lit(2)), F.list_value(lit(3), lit(4)))).alias("flat")
            )
            result = render(query)
            assert result.query == "SELECT FLATTEN(LIST_VALUE(LIST_VALUE(1, 2), LIST_VALUE(3, 4))) AS flat FROM t"
            assert result.params == {}


def describe_list_scalar_aggregates():
    """Test list scalar aggregate functions."""

    def describe_list_sum():
        """Test LIST_SUM function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_sum(col("amounts")).alias("total"))
            result = render(query)
            assert result.query == "SELECT LIST_SUM(amounts) AS total FROM t"
            assert result.params == {}

        def it_works_in_where_clause():
            query = ref("t").select(col("*")).where(F.list_sum(col("scores")) > lit(100))
            result = render(query)
            assert result.query == "SELECT * FROM t WHERE LIST_SUM(scores) > 100"
            assert result.params == {}

    def describe_list_avg():
        """Test LIST_AVG function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_avg(col("ratings")).alias("avg_rating"))
            result = render(query)
            assert result.query == "SELECT LIST_AVG(ratings) AS avg_rating FROM t"
            assert result.params == {}

        def it_works_in_having_clause():
            query = (
                ref("products")
                .select(col("category"), F.list_avg(col("prices")).alias("avg_price"))
                .group_by(col("category"))
                .having(F.list_avg(col("prices")) > lit(50))
            )
            result = render(query)
            assert result.query == (
                "SELECT category, LIST_AVG(prices) AS avg_price FROM products "
                "GROUP BY category HAVING LIST_AVG(prices) > 50"
            )
            assert result.params == {}

    def describe_list_min():
        """Test LIST_MIN function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_min(col("prices")).alias("min_price"))
            result = render(query)
            assert result.query == "SELECT LIST_MIN(prices) AS min_price FROM t"
            assert result.params == {}

        def it_combines_with_other_functions():
            query = ref("t").select(F.list_min(F.list_slice(col("prices"), lit(1), lit(5))).alias("min_top_5"))
            result = render(query)
            assert result.query == "SELECT LIST_MIN(LIST_SLICE(prices, 1, 5)) AS min_top_5 FROM t"
            assert result.params == {}

    def describe_list_max():
        """Test LIST_MAX function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_max(col("scores")).alias("max_score"))
            result = render(query)
            assert result.query == "SELECT LIST_MAX(scores) AS max_score FROM t"
            assert result.params == {}

        def it_works_in_order_by():
            query = ref("players").select(col("name"), col("scores")).order_by(F.list_max(col("scores")).desc())
            result = render(query)
            assert result.query == "SELECT name, scores FROM players ORDER BY LIST_MAX(scores) DESC"
            assert result.params == {}

    def describe_list_count():
        """Test LIST_COUNT function."""

        def it_renders_basic_usage():
            query = ref("t").select(F.list_count(col("items")).alias("num_items"))
            result = render(query)
            assert result.query == "SELECT LIST_COUNT(items) AS num_items FROM t"
            assert result.params == {}

        def it_works_in_where_clause():
            query = ref("t").select(col("*")).where(F.list_count(col("tags")) > lit(3))
            result = render(query)
            assert result.query == "SELECT * FROM t WHERE LIST_COUNT(tags) > 3"
            assert result.params == {}

        def it_combines_with_list_distinct():
            query = ref("t").select(F.list_count(F.list_distinct(col("tags"))).alias("unique_tag_count"))
            result = render(query)
            assert result.query == "SELECT LIST_COUNT(LIST_DISTINCT(tags)) AS unique_tag_count FROM t"
            assert result.params == {}
