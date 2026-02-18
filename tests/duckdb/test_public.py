"""Tests for vw.duckdb.public module - factory functions."""

from vw.core.case import When
from vw.core.states import (
    CTE,
    Cube,
    Exists,
    Function,
    GroupingSets,
    Literal,
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
from vw.duckdb.states import Column, StructInsert, StructPack


def describe_factory_functions():
    """Tests for factory functions (ref, col, param, lit, etc.)."""

    def describe_ref():
        """Tests for ref() factory."""

        def it_creates_rowset_with_reference_state():
            """Test ref() creates a RowSet with Reference state."""
            result = ref("users")

            assert isinstance(result.state, Reference)

            # Verify it renders correctly
            sql = render(result)
            assert sql.query == "FROM users"

    def describe_col():
        """Tests for col() factory."""

        def it_creates_expression_with_column_state():
            """Test col() creates an Expression with Column state."""
            result = col("name")

            assert isinstance(result.state, Column)

    def describe_param():
        """Tests for param() factory."""

        def it_creates_expression_with_parameter_state():
            """Test param() creates an Expression with Parameter state."""
            result = param("age", 18)

            assert isinstance(result.state, Parameter)

    def describe_lit():
        """Tests for lit() factory."""

        def it_renders_literal_value_correctly():
            """Test lit() creates a literal value and renders correctly."""
            # Verify it works in a query
            query = ref("test").select(lit(42).alias("num"))
            sql = render(query)
            assert sql.query == "SELECT 42 AS num FROM test"

    def describe_when():
        """Tests for when() factory."""

        def it_creates_when_builder():
            """Test when() creates a When builder."""
            condition = col("age") >= lit(18)
            result = when(condition)

            assert isinstance(result, When)
            assert result.prior_whens == ()

    def describe_exists():
        """Tests for exists() factory."""

        def it_creates_expression_with_exists_state():
            """Test exists() creates an Expression with Exists state."""
            # Use a Reference as subquery so it gets wrapped with SELECT *
            subquery_ref = ref("orders")
            result = exists(subquery_ref)

            assert isinstance(result.state, Exists)

            # Verify it renders correctly with Reference subquery
            query = ref("users").select(col("id")).where(result)
            sql = render(query)
            assert sql.query == "SELECT id FROM users WHERE EXISTS (SELECT * FROM orders)"

        def it_works_with_statement_subquery():
            """Test exists() with a Statement subquery."""
            subquery = ref("orders").where(col("user_id") == col("users.id"))
            result = exists(subquery)

            # Verify Statement subquery renders without SELECT wrapper
            query = ref("users").select(col("id")).where(result)
            sql = render(query)
            # Statement already has FROM, so just wrapped in parentheses
            assert sql.query == "SELECT id FROM users WHERE EXISTS (FROM orders WHERE user_id = users.id)"

    def describe_functions_instance():
        """Tests for Functions instance."""

        def it_provides_f_instance():
            """Test F is a Functions instance with standard functions."""
            from vw.duckdb.public import Functions

            assert isinstance(F, Functions)

    def describe_values():
        """Tests for values() factory."""

        def it_creates_rowset_with_values_state():
            """Test values() creates a RowSet with Values state."""
            result = values("t", {"id": 1, "name": "Alice"})

            assert isinstance(result.state, Values)
            assert result.state.alias == "t"
            assert len(result.state.rows) == 1

        def it_handles_multiple_rows():
            """Test values() with multiple rows."""
            result = values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})

            assert isinstance(result.state, Values)
            assert len(result.state.rows) == 2

        def it_renders_correctly():
            """Test values() renders correctly."""
            query = values("t", {"id": 1, "name": "Alice"}).select(col("id"), col("name"))
            sql = render(query)

            assert sql.query == "SELECT id, name FROM (VALUES ($_v0_0_id, $_v0_1_name)) AS t(id, name)"
            assert sql.params == {"_v0_0_id": 1, "_v0_1_name": "Alice"}

        def it_works_with_expressions():
            """Test values() with Expression objects."""
            result = values("t", {"id": lit(1), "name": lit("Alice")}, {"id": lit(2), "name": lit("Bob")})

            assert isinstance(result.state, Values)
            assert len(result.state.rows) == 2

            query = result.select(col("id"), col("name"))
            sql = render(query)

            assert sql.query == "SELECT id, name FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)"

    def describe_cte():
        """Tests for cte() factory."""

        def it_creates_rowset_with_cte_state():
            """Test cte() creates a RowSet with CTE state."""
            query = ref("users").select(col("id")).where(col("active") == lit(True))
            result = cte("active_users", query)

            assert isinstance(result.state, CTE)
            assert result.state.name == "active_users"
            assert result.state.recursive is False

        def it_supports_recursive_flag():
            """Test cte() with recursive=True."""
            anchor = ref("items").select(col("*")).where(col("parent_id").is_null())
            result = cte("tree", anchor, recursive=True)

            assert isinstance(result.state, CTE)
            assert result.state.recursive is True

        def it_renders_correctly():
            """Test cte() renders correctly."""
            active_users = cte("active_users", ref("users").select(col("*")).where(col("active") == lit(True)))
            query = active_users.select(col("id"), col("name"))

            sql = render(query)
            assert "WITH active_users AS" in sql.query
            assert "SELECT id, name FROM active_users" in sql.query

        def it_works_with_reference():
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

    def describe_rollup():
        """Tests for rollup() factory."""

        def it_creates_expression_with_rollup_state():
            """Test rollup() creates an Expression with Rollup state."""
            result = rollup(col("region"), col("product"))

            assert isinstance(result.state, Rollup)
            assert len(result.state.columns) == 2

        def it_renders_correctly_in_group_by():
            """Test rollup() renders correctly in GROUP BY."""
            query = (
                ref("sales")
                .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
                .group_by(rollup(col("region"), col("product")))
            )

            sql = render(query)
            assert "GROUP BY ROLLUP (region, product)" in sql.query

        def it_works_with_single_column():
            """Test rollup() with a single column."""
            query = (
                ref("sales").select(col("region"), F.sum(col("amount")).alias("total")).group_by(rollup(col("region")))
            )

            sql = render(query)
            assert "GROUP BY ROLLUP (region)" in sql.query

    def describe_cube():
        """Tests for cube() factory."""

        def it_creates_expression_with_cube_state():
            """Test cube() creates an Expression with Cube state."""
            result = cube(col("region"), col("product"))

            assert isinstance(result.state, Cube)
            assert len(result.state.columns) == 2

        def it_renders_correctly_in_group_by():
            """Test cube() renders correctly in GROUP BY."""
            query = (
                ref("sales")
                .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
                .group_by(cube(col("region"), col("product")))
            )

            sql = render(query)
            assert "GROUP BY CUBE (region, product)" in sql.query

        def it_works_with_single_column():
            """Test cube() with a single column."""
            query = (
                ref("sales").select(col("region"), F.sum(col("amount")).alias("total")).group_by(cube(col("region")))
            )

            sql = render(query)
            assert "GROUP BY CUBE (region)" in sql.query

    def describe_grouping_sets():
        """Tests for grouping_sets() factory."""

        def it_creates_expression_with_grouping_sets_state():
            """Test grouping_sets() creates an Expression with GroupingSets state."""
            result = grouping_sets((col("region"),), (col("product"),), ())

            assert isinstance(result.state, GroupingSets)
            assert len(result.state.sets) == 3

        def it_renders_correctly_in_group_by():
            """Test grouping_sets() renders correctly in GROUP BY."""
            query = (
                ref("sales")
                .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
                .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
            )

            sql = render(query)
            assert "GROUP BY GROUPING SETS ((region, product), (region), ())" in sql.query

        def it_supports_empty_set_for_grand_total():
            """Test grouping_sets() with an empty set (grand total)."""
            query = (
                ref("sales")
                .select(col("region"), F.sum(col("amount")).alias("total"))
                .group_by(grouping_sets((col("region"),), ()))
            )

            sql = render(query)
            assert "GROUP BY GROUPING SETS ((region), ())" in sql.query

        def it_works_with_multiple_columns_in_sets():
            """Test grouping_sets() with multiple columns in sets."""
            query = (
                ref("sales")
                .select(col("year"), col("quarter"), col("region"), F.sum(col("amount")).alias("total"))
                .group_by(grouping_sets((col("year"), col("quarter")), (col("year"),), (col("region"),)))
            )

            sql = render(query)
            assert "GROUP BY GROUPING SETS ((year, quarter), (year), (region))" in sql.query


def describe_list_functions():
    """Tests for DuckDB list/array functions."""

    def describe_list_construction():
        """Tests for list construction functions."""

        def describe_list_value():
            """Tests for F.list_value()."""

            def it_creates_list_function_state():
                """Test F.list_value() creates a list from elements."""
                result = F.list_value(lit(1), lit(2), lit(3))

                assert result.state == Function(
                    name="LIST_VALUE", args=(Literal(value=1), Literal(value=2), Literal(value=3))
                )

            def it_renders_correctly():
                """Test F.list_value() renders correctly."""
                query = ref("t").select(F.list_value(lit(1), lit(2), lit(3)).alias("nums"))
                sql = render(query)

                assert sql.query == "SELECT LIST_VALUE(1, 2, 3) AS nums FROM t"
                assert sql.params == {}

        def describe_list_agg():
            """Tests for F.list_agg()."""

            def it_creates_list_aggregation_state():
                """Test F.list_agg() creates a list aggregation."""
                result = F.list_agg(col("name"))

                assert result.state == Function(
                    name="LIST_AGG", args=(Column(name="name", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_agg() renders correctly."""
                query = ref("t").select(F.list_agg(col("name")).alias("names"))
                sql = render(query)

                assert sql.query == "SELECT LIST_AGG(name) AS names FROM t"

            def it_supports_distinct():
                """Test F.list_agg() with distinct=True."""
                query = ref("t").select(F.list_agg(col("tag"), distinct=True).alias("unique_tags"))
                sql = render(query)

                assert sql.query == "SELECT LIST_AGG(DISTINCT tag) AS unique_tags FROM t"

            def it_supports_order_by():
                """Test F.list_agg() with ORDER BY."""
                query = ref("t").select(F.list_agg(col("name"), order_by=[col("name").asc()]).alias("sorted_names"))
                sql = render(query)

                assert sql.query == "SELECT LIST_AGG(name ORDER BY name ASC) AS sorted_names FROM t"

    def describe_list_operations():
        """Tests for list access and manipulation functions."""

        def describe_list_extract():
            """Tests for F.list_extract()."""

            def it_creates_function_state():
                """Test F.list_extract() accesses list element."""
                result = F.list_extract(col("tags"), lit(1))

                assert result.state == Function(
                    name="LIST_EXTRACT", args=(Column(name="tags", exclude=None, replace=None), Literal(value=1))
                )

            def it_renders_correctly():
                """Test F.list_extract() renders correctly."""
                query = ref("t").select(F.list_extract(col("tags"), lit(1)).alias("first_tag"))
                sql = render(query)

                assert sql.query == "SELECT LIST_EXTRACT(tags, 1) AS first_tag FROM t"

        def describe_list_slice():
            """Tests for F.list_slice()."""

            def it_creates_function_state():
                """Test F.list_slice() slices list."""
                result = F.list_slice(col("items"), lit(1), lit(3))

                assert result.state == Function(
                    name="LIST_SLICE",
                    args=(Column(name="items", exclude=None, replace=None), Literal(value=1), Literal(value=3)),
                )

            def it_renders_correctly():
                """Test F.list_slice() renders correctly."""
                query = ref("t").select(F.list_slice(col("items"), lit(1), lit(3)).alias("slice"))
                sql = render(query)

                assert sql.query == "SELECT LIST_SLICE(items, 1, 3) AS slice FROM t"

            def it_supports_step_parameter():
                """Test F.list_slice() with step parameter."""
                query = ref("t").select(F.list_slice(col("nums"), lit(1), lit(10), lit(2)).alias("evens"))
                sql = render(query)

                assert sql.query == "SELECT LIST_SLICE(nums, 1, 10, 2) AS evens FROM t"

        def describe_list_contains():
            """Tests for F.list_contains()."""

            def it_creates_function_state():
                """Test F.list_contains() checks membership."""
                result = F.list_contains(col("tags"), lit("python"))

                assert result.state == Function(
                    name="LIST_CONTAINS",
                    args=(Column(name="tags", exclude=None, replace=None), Literal(value="python")),
                )

            def it_renders_correctly():
                """Test F.list_contains() renders correctly."""
                query = ref("t").select(F.list_contains(col("tags"), lit("python")).alias("has_python"))
                sql = render(query)

                assert sql.query == "SELECT LIST_CONTAINS(tags, 'python') AS has_python FROM t"

        def describe_list_concat():
            """Tests for F.list_concat()."""

            def it_creates_function_state():
                """Test F.list_concat() concatenates lists."""
                result = F.list_concat(col("list1"), col("list2"))

                assert result.state == Function(
                    name="LIST_CONCAT",
                    args=(
                        Column(name="list1", exclude=None, replace=None),
                        Column(name="list2", exclude=None, replace=None),
                    ),
                )

            def it_renders_correctly():
                """Test F.list_concat() renders correctly."""
                query = ref("t").select(F.list_concat(col("list1"), col("list2")).alias("combined"))
                sql = render(query)

                assert sql.query == "SELECT LIST_CONCAT(list1, list2) AS combined FROM t"

        def describe_list_append():
            """Tests for F.list_append()."""

            def it_creates_function_state():
                """Test F.list_append() appends element."""
                result = F.list_append(col("tags"), lit("new"))

                assert result.state == Function(
                    name="LIST_APPEND", args=(Column(name="tags", exclude=None, replace=None), Literal(value="new"))
                )

            def it_renders_correctly():
                """Test F.list_append() renders correctly."""
                query = ref("t").select(F.list_append(col("tags"), lit("new")).alias("updated_tags"))
                sql = render(query)

                assert sql.query == "SELECT LIST_APPEND(tags, 'new') AS updated_tags FROM t"

        def describe_list_prepend():
            """Tests for F.list_prepend()."""

            def it_creates_function_state():
                """Test F.list_prepend() prepends element."""
                result = F.list_prepend(lit("first"), col("tags"))

                assert result.state == Function(
                    name="LIST_PREPEND", args=(Literal(value="first"), Column(name="tags", exclude=None, replace=None))
                )

            def it_renders_correctly():
                """Test F.list_prepend() renders correctly."""
                query = ref("t").select(F.list_prepend(lit("first"), col("tags")).alias("updated_tags"))
                sql = render(query)

                assert sql.query == "SELECT LIST_PREPEND('first', tags) AS updated_tags FROM t"

    def describe_list_transformation():
        """Tests for list transformation functions."""

        def describe_list_sort():
            """Tests for F.list_sort()."""

            def it_creates_function_state():
                """Test F.list_sort() sorts list."""
                result = F.list_sort(col("numbers"))

                assert result.state == Function(
                    name="LIST_SORT", args=(Column(name="numbers", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_sort() renders correctly."""
                query = ref("t").select(F.list_sort(col("numbers")).alias("sorted"))
                sql = render(query)

                assert sql.query == "SELECT LIST_SORT(numbers) AS sorted FROM t"

        def describe_list_reverse():
            """Tests for F.list_reverse()."""

            def it_creates_function_state():
                """Test F.list_reverse() reverses list."""
                result = F.list_reverse(col("items"))

                assert result.state == Function(
                    name="LIST_REVERSE", args=(Column(name="items", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_reverse() renders correctly."""
                query = ref("t").select(F.list_reverse(col("items")).alias("reversed"))
                sql = render(query)

                assert sql.query == "SELECT LIST_REVERSE(items) AS reversed FROM t"

        def describe_list_distinct():
            """Tests for F.list_distinct()."""

            def it_creates_function_state():
                """Test F.list_distinct() removes duplicates."""
                result = F.list_distinct(col("tags"))

                assert result.state == Function(
                    name="LIST_DISTINCT", args=(Column(name="tags", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_distinct() renders correctly."""
                query = ref("t").select(F.list_distinct(col("tags")).alias("unique_tags"))
                sql = render(query)

                assert sql.query == "SELECT LIST_DISTINCT(tags) AS unique_tags FROM t"

        def describe_list_has_any():
            """Tests for F.list_has_any()."""

            def it_creates_function_state():
                """Test F.list_has_any() checks overlap."""
                result = F.list_has_any(col("user_tags"), col("required_tags"))

                assert result.state == Function(
                    name="LIST_HAS_ANY",
                    args=(
                        Column(name="user_tags", exclude=None, replace=None),
                        Column(name="required_tags", exclude=None, replace=None),
                    ),
                )

            def it_renders_correctly():
                """Test F.list_has_any() renders correctly."""
                query = ref("t").select(F.list_has_any(col("user_tags"), col("required_tags")).alias("has_match"))
                sql = render(query)

                assert sql.query == "SELECT LIST_HAS_ANY(user_tags, required_tags) AS has_match FROM t"

        def describe_list_has_all():
            """Tests for F.list_has_all()."""

            def it_creates_function_state():
                """Test F.list_has_all() checks containment."""
                result = F.list_has_all(col("permissions"), col("required_perms"))

                assert result.state == Function(
                    name="LIST_HAS_ALL",
                    args=(
                        Column(name="permissions", exclude=None, replace=None),
                        Column(name="required_perms", exclude=None, replace=None),
                    ),
                )

            def it_renders_correctly():
                """Test F.list_has_all() renders correctly."""
                query = ref("t").select(F.list_has_all(col("permissions"), col("required_perms")).alias("has_all"))
                sql = render(query)

                assert sql.query == "SELECT LIST_HAS_ALL(permissions, required_perms) AS has_all FROM t"

        def describe_flatten():
            """Tests for F.flatten()."""

            def it_creates_function_state():
                """Test F.flatten() flattens nested lists."""
                result = F.flatten(col("nested_lists"))

                assert result.state == Function(
                    name="FLATTEN", args=(Column(name="nested_lists", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.flatten() renders correctly."""
                query = ref("t").select(F.flatten(col("nested_lists")).alias("flat"))
                sql = render(query)

                assert sql.query == "SELECT FLATTEN(nested_lists) AS flat FROM t"

    def describe_list_aggregation():
        """Tests for list scalar aggregate functions."""

        def describe_list_sum():
            """Tests for F.list_sum()."""

            def it_creates_function_state():
                """Test F.list_sum() sums list elements."""
                result = F.list_sum(col("amounts"))

                assert result.state == Function(
                    name="LIST_SUM", args=(Column(name="amounts", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_sum() renders correctly."""
                query = ref("t").select(F.list_sum(col("amounts")).alias("total"))
                sql = render(query)

                assert sql.query == "SELECT LIST_SUM(amounts) AS total FROM t"

        def describe_list_avg():
            """Tests for F.list_avg()."""

            def it_creates_function_state():
                """Test F.list_avg() averages list elements."""
                result = F.list_avg(col("ratings"))

                assert result.state == Function(
                    name="LIST_AVG", args=(Column(name="ratings", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_avg() renders correctly."""
                query = ref("t").select(F.list_avg(col("ratings")).alias("avg_rating"))
                sql = render(query)

                assert sql.query == "SELECT LIST_AVG(ratings) AS avg_rating FROM t"

        def describe_list_min():
            """Tests for F.list_min()."""

            def it_creates_function_state():
                """Test F.list_min() finds minimum element."""
                result = F.list_min(col("prices"))

                assert result.state == Function(
                    name="LIST_MIN", args=(Column(name="prices", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_min() renders correctly."""
                query = ref("t").select(F.list_min(col("prices")).alias("min_price"))
                sql = render(query)

                assert sql.query == "SELECT LIST_MIN(prices) AS min_price FROM t"

        def describe_list_max():
            """Tests for F.list_max()."""

            def it_creates_function_state():
                """Test F.list_max() finds maximum element."""
                result = F.list_max(col("scores"))

                assert result.state == Function(
                    name="LIST_MAX", args=(Column(name="scores", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_max() renders correctly."""
                query = ref("t").select(F.list_max(col("scores")).alias("max_score"))
                sql = render(query)

                assert sql.query == "SELECT LIST_MAX(scores) AS max_score FROM t"

        def describe_list_count():
            """Tests for F.list_count()."""

            def it_creates_function_state():
                """Test F.list_count() counts list elements."""
                result = F.list_count(col("items"))

                assert result.state == Function(
                    name="LIST_COUNT", args=(Column(name="items", exclude=None, replace=None),)
                )

            def it_renders_correctly():
                """Test F.list_count() renders correctly."""
                query = ref("t").select(F.list_count(col("items")).alias("num_items"))
                sql = render(query)

                assert sql.query == "SELECT LIST_COUNT(items) AS num_items FROM t"


def describe_shared_aggregate_functions():
    """Tests for shared aggregate functions (from mixins)."""

    def describe_array_agg():
        """Tests for F.array_agg()."""

        def it_is_available_from_mixin():
            """Test F.array_agg() is available (from mixin)."""
            result = F.array_agg(col("name"))

            assert result.state == Function(name="ARRAY_AGG", args=(Column(name="name", exclude=None, replace=None),))

        def it_renders_correctly():
            """Test F.array_agg() renders correctly."""
            query = ref("t").select(F.array_agg(col("name")).alias("names"))
            sql = render(query)

            assert sql.query == "SELECT ARRAY_AGG(name) AS names FROM t"

    def describe_unnest():
        """Tests for F.unnest()."""

        def it_is_available_from_mixin():
            """Test F.unnest() is available (from mixin)."""
            result = F.unnest(col("array_col"))

            assert result.state == Function(name="UNNEST", args=(Column(name="array_col", exclude=None, replace=None),))

        def it_renders_correctly():
            """Test F.unnest() renders correctly."""
            query = ref("data").select(F.unnest(col("array_col")))
            sql = render(query)

            assert sql.query == "SELECT UNNEST(array_col) FROM data"

    def describe_string_agg():
        """Tests for F.string_agg()."""

        def it_is_available_from_mixin():
            """Test F.string_agg() is available (from mixin)."""
            result = F.string_agg(col("name"), lit(", "))

            assert result.state == Function(
                name="STRING_AGG", args=(Column(name="name", exclude=None, replace=None), Literal(value=", "))
            )

        def it_renders_correctly():
            """Test F.string_agg() renders correctly."""
            query = ref("t").select(F.string_agg(col("name"), lit(", ")).alias("names"))
            sql = render(query)

            assert sql.query == "SELECT STRING_AGG(name, ', ') AS names FROM t"

    def describe_json_agg():
        """Tests for F.json_agg()."""

        def it_is_available_from_mixin():
            """Test F.json_agg() is available (from mixin)."""
            result = F.json_agg(col("data"))

            assert result.state == Function(name="JSON_AGG", args=(Column(name="data", exclude=None, replace=None),))

        def it_renders_correctly():
            """Test F.json_agg() renders correctly."""
            query = ref("t").select(F.json_agg(col("data")).alias("json_data"))
            sql = render(query)

            assert sql.query == "SELECT JSON_AGG(data) AS json_data FROM t"

    def describe_bit_and():
        """Tests for F.bit_and()."""

        def it_is_available_from_mixin():
            """Test F.bit_and() is available (from mixin)."""
            result = F.bit_and(col("flags"))

            assert result.state == Function(name="BIT_AND", args=(Column(name="flags", exclude=None, replace=None),))

    def describe_bit_or():
        """Tests for F.bit_or()."""

        def it_is_available_from_mixin():
            """Test F.bit_or() is available (from mixin)."""
            result = F.bit_or(col("flags"))

            assert result.state == Function(name="BIT_OR", args=(Column(name="flags", exclude=None, replace=None),))

    def describe_bool_and():
        """Tests for F.bool_and()."""

        def it_is_available_from_mixin():
            """Test F.bool_and() is available (from mixin)."""
            result = F.bool_and(col("is_active"))

            assert result.state == Function(
                name="BOOL_AND", args=(Column(name="is_active", exclude=None, replace=None),)
            )

    def describe_bool_or():
        """Tests for F.bool_or()."""

        def it_is_available_from_mixin():
            """Test F.bool_or() is available (from mixin)."""
            result = F.bool_or(col("has_error"))

            assert result.state == Function(
                name="BOOL_OR", args=(Column(name="has_error", exclude=None, replace=None),)
            )


def describe_struct_functions():
    """Tests for DuckDB struct functions."""

    def describe_struct_pack():
        """Tests for F.struct_pack()."""

        def it_creates_struct_pack_state():
            result = F.struct_pack({"name": lit("Alice"), "age": lit(30)})

            assert result.state == StructPack(fields=(("name", Literal(value="Alice")), ("age", Literal(value=30))))

        def it_renders_correctly():
            query = ref("t").select(F.struct_pack({"name": lit("Alice"), "age": lit(30)}).alias("person"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_PACK(name := 'Alice', age := 30) AS person FROM t"
            assert sql.params == {}

        def it_renders_single_field():
            query = ref("t").select(F.struct_pack({"x": lit(1)}).alias("s"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_PACK(x := 1) AS s FROM t"

        def it_renders_with_column_args():
            result = F.struct_pack({"x": col("x_val"), "y": col("y_val")})

            assert result.state == StructPack(
                fields=(
                    ("x", Column(name="x_val", exclude=None, replace=None)),
                    ("y", Column(name="y_val", exclude=None, replace=None)),
                )
            )

    def describe_struct_extract():
        """Tests for F.struct_extract()."""

        def it_creates_function_state():
            result = F.struct_extract(col("address"), "city")

            assert result.state == Function(
                name="STRUCT_EXTRACT",
                args=(Column(name="address", exclude=None, replace=None), Literal(value="city")),
            )

        def it_renders_correctly():
            query = ref("t").select(F.struct_extract(col("address"), "city").alias("city"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_EXTRACT(address, 'city') AS city FROM t"
            assert sql.params == {}

    def describe_struct_insert():
        """Tests for F.struct_insert()."""

        def it_creates_struct_insert_state():
            result = F.struct_insert(col("s"), {"score": lit(100)})

            assert result.state == StructInsert(
                struct=Column(name="s", exclude=None, replace=None),
                fields=(("score", Literal(value=100)),),
            )

        def it_renders_correctly():
            query = ref("t").select(F.struct_insert(col("s"), {"score": lit(100)}).alias("updated"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_INSERT(s, score := 100) AS updated FROM t"
            assert sql.params == {}

        def it_renders_multiple_fields():
            query = ref("t").select(
                F.struct_insert(col("addr"), {"zip": lit("12345"), "country": lit("US")}).alias("full")
            )
            sql = render(query)

            assert sql.query == "SELECT STRUCT_INSERT(addr, zip := '12345', country := 'US') AS full FROM t"

    def describe_struct_keys():
        """Tests for F.struct_keys()."""

        def it_creates_function_state():
            result = F.struct_keys(col("s"))

            assert result.state == Function(name="STRUCT_KEYS", args=(Column(name="s", exclude=None, replace=None),))

        def it_renders_correctly():
            query = ref("t").select(F.struct_keys(col("s")).alias("keys"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_KEYS(s) AS keys FROM t"
            assert sql.params == {}

    def describe_struct_values():
        """Tests for F.struct_values()."""

        def it_creates_function_state():
            result = F.struct_values(col("s"))

            assert result.state == Function(name="STRUCT_VALUES", args=(Column(name="s", exclude=None, replace=None),))

        def it_renders_correctly():
            query = ref("t").select(F.struct_values(col("s")).alias("vals"))
            sql = render(query)

            assert sql.query == "SELECT STRUCT_VALUES(s) AS vals FROM t"
            assert sql.params == {}


def describe_duckdb_string_functions():
    """Tests for DuckDB-specific string functions."""

    def describe_regexp_matches():
        """Tests for F.regexp_matches()."""

        def it_creates_function_state():
            result = F.regexp_matches(col("email"), lit(r"\d+"))

            assert result.state == Function(
                name="REGEXP_MATCHES",
                args=(Column(name="email", exclude=None, replace=None), Literal(value=r"\d+")),
            )

        def it_renders_correctly():
            query = ref("t").select(F.regexp_matches(col("email"), lit(r"\d+")).alias("match"))
            sql = render(query)

            assert sql.query == r"SELECT REGEXP_MATCHES(email, '\d+') AS match FROM t"
            assert sql.params == {}

    def describe_regexp_replace():
        """Tests for F.regexp_replace()."""

        def it_creates_function_state_without_flags():
            result = F.regexp_replace(col("text"), lit(r"\s+"), lit(" "))

            assert result.state == Function(
                name="REGEXP_REPLACE",
                args=(
                    Column(name="text", exclude=None, replace=None),
                    Literal(value=r"\s+"),
                    Literal(value=" "),
                ),
            )

        def it_creates_function_state_with_flags():
            result = F.regexp_replace(col("text"), lit(r"\s+"), lit(" "), lit("g"))

            assert result.state == Function(
                name="REGEXP_REPLACE",
                args=(
                    Column(name="text", exclude=None, replace=None),
                    Literal(value=r"\s+"),
                    Literal(value=" "),
                    Literal(value="g"),
                ),
            )

        def it_renders_without_flags():
            query = ref("t").select(F.regexp_replace(col("text"), lit(r"\s+"), lit(" ")).alias("clean"))
            sql = render(query)

            assert sql.query == r"SELECT REGEXP_REPLACE(text, '\s+', ' ') AS clean FROM t"

        def it_renders_with_flags():
            query = ref("t").select(F.regexp_replace(col("text"), lit(r"[aeiou]"), lit("*"), lit("g")).alias("clean"))
            sql = render(query)

            assert sql.query == r"SELECT REGEXP_REPLACE(text, '[aeiou]', '*', 'g') AS clean FROM t"

    def describe_regexp_extract():
        """Tests for F.regexp_extract()."""

        def it_creates_function_state_without_group():
            result = F.regexp_extract(col("text"), lit(r"\d+"))

            assert result.state == Function(
                name="REGEXP_EXTRACT",
                args=(Column(name="text", exclude=None, replace=None), Literal(value=r"\d+")),
            )

        def it_creates_function_state_with_group():
            result = F.regexp_extract(col("text"), lit(r"(\d+)"), lit(1))

            assert result.state == Function(
                name="REGEXP_EXTRACT",
                args=(
                    Column(name="text", exclude=None, replace=None),
                    Literal(value=r"(\d+)"),
                    Literal(value=1),
                ),
            )

        def it_renders_without_group():
            query = ref("t").select(F.regexp_extract(col("text"), lit(r"\d+")).alias("digits"))
            sql = render(query)

            assert sql.query == r"SELECT REGEXP_EXTRACT(text, '\d+') AS digits FROM t"

        def it_renders_with_group():
            query = ref("t").select(F.regexp_extract(col("text"), lit(r"(\d+)"), lit(1)).alias("group1"))
            sql = render(query)

            assert sql.query == r"SELECT REGEXP_EXTRACT(text, '(\d+)', 1) AS group1 FROM t"

    def describe_string_split():
        """Tests for F.string_split()."""

        def it_creates_function_state():
            result = F.string_split(col("tags"), lit(","))

            assert result.state == Function(
                name="STRING_SPLIT",
                args=(Column(name="tags", exclude=None, replace=None), Literal(value=",")),
            )

        def it_renders_correctly():
            query = ref("t").select(F.string_split(col("tags"), lit(",")).alias("tag_list"))
            sql = render(query)

            assert sql.query == "SELECT STRING_SPLIT(tags, ',') AS tag_list FROM t"
            assert sql.params == {}

    def describe_string_split_regex():
        """Tests for F.string_split_regex()."""

        def it_creates_function_state():
            result = F.string_split_regex(col("text"), lit(r"\s+"))

            assert result.state == Function(
                name="STRING_SPLIT_REGEX",
                args=(Column(name="text", exclude=None, replace=None), Literal(value=r"\s+")),
            )

        def it_renders_correctly():
            query = ref("t").select(F.string_split_regex(col("text"), lit(r"\s+")).alias("words"))
            sql = render(query)

            assert sql.query == r"SELECT STRING_SPLIT_REGEX(text, '\s+') AS words FROM t"
            assert sql.params == {}


def describe_duckdb_datetime_functions():
    """Tests for DuckDB-specific date/time functions."""

    def describe_date_diff():
        """Tests for F.date_diff()."""

        def it_creates_function_state():
            result = F.date_diff(lit("day"), col("start_date"), col("end_date"))

            assert result.state == Function(
                name="DATE_DIFF",
                args=(
                    Literal(value="day"),
                    Column(name="start_date", exclude=None, replace=None),
                    Column(name="end_date", exclude=None, replace=None),
                ),
            )

        def it_renders_correctly():
            query = ref("t").select(F.date_diff(lit("day"), col("start_date"), col("end_date")).alias("days"))
            sql = render(query)

            assert sql.query == "SELECT DATE_DIFF('day', start_date, end_date) AS days FROM t"
            assert sql.params == {}

    def describe_date_part():
        """Tests for F.date_part()."""

        def it_creates_function_state():
            result = F.date_part(lit("year"), col("created_at"))

            assert result.state == Function(
                name="DATE_PART",
                args=(Literal(value="year"), Column(name="created_at", exclude=None, replace=None)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.date_part(lit("year"), col("created_at")).alias("year"))
            sql = render(query)

            assert sql.query == "SELECT DATE_PART('year', created_at) AS year FROM t"
            assert sql.params == {}

    def describe_make_date():
        """Tests for F.make_date()."""

        def it_creates_function_state():
            result = F.make_date(lit(2024), lit(1), lit(15))

            assert result.state == Function(
                name="MAKE_DATE",
                args=(Literal(value=2024), Literal(value=1), Literal(value=15)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.make_date(lit(2024), lit(1), lit(15)).alias("d"))
            sql = render(query)

            assert sql.query == "SELECT MAKE_DATE(2024, 1, 15) AS d FROM t"
            assert sql.params == {}

    def describe_make_time():
        """Tests for F.make_time()."""

        def it_creates_function_state():
            result = F.make_time(lit(14), lit(30), lit(0))

            assert result.state == Function(
                name="MAKE_TIME",
                args=(Literal(value=14), Literal(value=30), Literal(value=0)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.make_time(lit(14), lit(30), lit(0)).alias("t"))
            sql = render(query)

            assert sql.query == "SELECT MAKE_TIME(14, 30, 0) AS t FROM t"
            assert sql.params == {}

    def describe_make_timestamp():
        """Tests for F.make_timestamp()."""

        def it_creates_function_state():
            result = F.make_timestamp(lit(2024), lit(1), lit(15), lit(14), lit(30), lit(0))

            assert result.state == Function(
                name="MAKE_TIMESTAMP",
                args=(
                    Literal(value=2024),
                    Literal(value=1),
                    Literal(value=15),
                    Literal(value=14),
                    Literal(value=30),
                    Literal(value=0),
                ),
            )

        def it_renders_correctly():
            query = ref("t").select(F.make_timestamp(lit(2024), lit(1), lit(15), lit(14), lit(30), lit(0)).alias("ts"))
            sql = render(query)

            assert sql.query == "SELECT MAKE_TIMESTAMP(2024, 1, 15, 14, 30, 0) AS ts FROM t"
            assert sql.params == {}


def describe_duckdb_statistical_functions():
    """Tests for DuckDB statistical aggregate functions."""

    def describe_approx_count_distinct():
        """Tests for F.approx_count_distinct()."""

        def it_creates_function_state():
            result = F.approx_count_distinct(col("user_id"))

            assert result.state == Function(
                name="APPROX_COUNT_DISTINCT", args=(Column(name="user_id", exclude=None, replace=None),)
            )

        def it_renders_correctly():
            query = ref("t").select(F.approx_count_distinct(col("user_id")).alias("approx_users"))
            sql = render(query)

            assert sql.query == "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx_users FROM t"
            assert sql.params == {}

    def describe_approx_quantile():
        """Tests for F.approx_quantile()."""

        def it_creates_function_state():
            result = F.approx_quantile(col("latency"), lit(0.95))

            assert result.state == Function(
                name="APPROX_QUANTILE",
                args=(Column(name="latency", exclude=None, replace=None), Literal(value=0.95)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.approx_quantile(col("latency"), lit(0.95)).alias("p95"))
            sql = render(query)

            assert sql.query == "SELECT APPROX_QUANTILE(latency, 0.95) AS p95 FROM t"
            assert sql.params == {}

    def describe_mode():
        """Tests for F.mode()."""

        def it_creates_function_state():
            result = F.mode(col("category"))

            assert result.state == Function(name="MODE", args=(Column(name="category", exclude=None, replace=None),))

        def it_renders_correctly():
            query = ref("t").select(F.mode(col("category")).alias("most_common"))
            sql = render(query)

            assert sql.query == "SELECT MODE(category) AS most_common FROM t"
            assert sql.params == {}

    def describe_median():
        """Tests for F.median()."""

        def it_creates_function_state():
            result = F.median(col("age"))

            assert result.state == Function(name="MEDIAN", args=(Column(name="age", exclude=None, replace=None),))

        def it_renders_correctly():
            query = ref("t").select(F.median(col("age")).alias("median_age"))
            sql = render(query)

            assert sql.query == "SELECT MEDIAN(age) AS median_age FROM t"
            assert sql.params == {}

    def describe_quantile():
        """Tests for F.quantile()."""

        def it_creates_function_state():
            result = F.quantile(col("response_time"), lit(0.99))

            assert result.state == Function(
                name="QUANTILE",
                args=(Column(name="response_time", exclude=None, replace=None), Literal(value=0.99)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.quantile(col("response_time"), lit(0.99)).alias("p99"))
            sql = render(query)

            assert sql.query == "SELECT QUANTILE(response_time, 0.99) AS p99 FROM t"
            assert sql.params == {}

    def describe_reservoir_quantile():
        """Tests for F.reservoir_quantile()."""

        def it_creates_function_state():
            result = F.reservoir_quantile(col("value"), lit(0.5))

            assert result.state == Function(
                name="RESERVOIR_QUANTILE",
                args=(Column(name="value", exclude=None, replace=None), Literal(value=0.5)),
            )

        def it_renders_correctly():
            query = ref("t").select(F.reservoir_quantile(col("value"), lit(0.5)).alias("p50"))
            sql = render(query)

            assert sql.query == "SELECT RESERVOIR_QUANTILE(value, 0.5) AS p50 FROM t"
            assert sql.params == {}
