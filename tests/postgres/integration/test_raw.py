"""Integration tests for raw SQL API - tests full rendering pipeline."""

from tests.utils import sql
from vw.postgres import col, cte, param, raw, ref, render


def describe_raw_expr_rendering() -> None:
    """Test raw expression in full rendering context."""

    def it_renders_with_column_dependency() -> None:
        """Test raw.expr() with column reference."""
        expected_sql = """
        SELECT UPPER(username) AS upper_name
        FROM users
        """

        expr = raw.expr("UPPER({name})", name=col("username"))
        query = ref("users").select(expr.alias("upper_name"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_with_parameter_dependency() -> None:
        """Test raw.expr() with parameter."""
        expected_sql = """
        SELECT custom_func($input) AS result
        FROM data
        """

        expr = raw.expr("custom_func({val})", val=param("input", 42))
        query = ref("data").select(expr.alias("result"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"input": 42}

    def it_renders_with_multiple_dependencies() -> None:
        """Test raw.expr() with multiple dependencies."""
        expected_sql = """
        SELECT tags @> $tag AS match
        FROM posts
        """

        expr = raw.expr("{a} @> {b}", a=col("tags"), b=param("tag", "python"))
        query = ref("posts").select(expr.alias("match"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"tag": "python"}

    def it_renders_in_where_clause() -> None:
        """Test raw.expr() in WHERE clause."""
        expected_sql = """
        SELECT id
        FROM posts
        WHERE tsv @@ query
        """

        expr = raw.expr("{a} @@ {b}", a=col("tsv"), b=col("query"))
        query = ref("posts").select(col("id")).where(expr)

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_composes_with_regular_expressions() -> None:
        """Test raw.expr() mixed with regular vw expressions."""
        expected_sql = """
        SELECT id, title, ts_rank(tsv, query) AS rank
        FROM posts
        WHERE (published = $pub) AND (tsv @@ query)
        ORDER BY rank DESC
        """

        raw_rank = raw.expr("ts_rank({tsv}, {q})", tsv=col("tsv"), q=col("query"))
        raw_match = raw.expr("{a} @@ {b}", a=col("tsv"), b=col("query"))

        query = (
            ref("posts")
            .select(col("id"), col("title"), raw_rank.alias("rank"))
            .where((col("published") == param("pub", True)) & raw_match)
            .order_by(col("rank").desc())
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"pub": True}


def describe_raw_source_rendering() -> None:
    """Test raw source in full rendering context."""

    def it_renders_in_from_clause() -> None:
        """Test raw.rowset() in FROM clause."""
        expected_sql = """
        SELECT n
        FROM generate_series(1, 10) AS t(n)
        """

        query = raw.rowset("generate_series(1, 10) AS t(n)").select(col("n"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_with_parameter() -> None:
        """Test raw.rowset() with parameter."""
        expected_sql = """
        SELECT num
        FROM generate_series(1, $max) AS t(num)
        """

        query = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10)).select(col("num"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"max": 10}

    def it_renders_with_column_reference() -> None:
        """Test raw.rowset() with column reference."""
        expected_sql = """
        SELECT elem
        FROM LATERAL unnest(array_col) AS t(elem)
        """

        query = raw.rowset("LATERAL unnest({arr}) AS t(elem)", arr=col("array_col")).select(col("elem"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_json_table_function() -> None:
        """Test raw.rowset() with JSON table function."""
        expected_sql = """
        SELECT id, name
        FROM json_to_recordset(json_data) AS t(id INT, name TEXT)
        """

        query = raw.rowset("json_to_recordset({data}) AS t(id INT, name TEXT)", data=col("json_data")).select(
            col("id"), col("name")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_raw_in_joins() -> None:
    """Test raw expressions in join conditions."""

    def it_uses_raw_expr_in_join_on() -> None:
        """Test raw.expr() in JOIN ON clause."""
        expected_sql = """
        SELECT b.id, p.id
        FROM buildings AS b
        INNER JOIN parcels AS p ON (ST_Within(b.geom, p.geom))
        """

        buildings = ref("buildings").alias("b")
        parcels = ref("parcels").alias("p")

        query = buildings.join.inner(
            parcels, on=[raw.expr("ST_Within({a}, {b})", a=col("b.geom"), b=col("p.geom"))]
        ).select(col("b.id"), col("p.id"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_combines_raw_and_regular_join_conditions() -> None:
        """Test mixing raw.expr() with regular join conditions."""
        expected_sql = """
        SELECT p.id
        FROM posts AS p
        INNER JOIN tags AS t ON (p.id = t.post_id AND p.tags @> t.tag_array)
        """

        posts = ref("posts").alias("p")
        tags = ref("tags").alias("t")

        query = posts.join.inner(
            tags,
            on=[
                col("p.id") == col("t.post_id"),
                raw.expr("{a} @> {b}", a=col("p.tags"), b=col("t.tag_array")),
            ],
        ).select(col("p.id"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_raw_composability() -> None:
    """Test that raw SQL composes with regular vw API."""

    def it_mixes_raw_and_regular_in_select() -> None:
        """Test raw expressions mixed with regular columns in SELECT."""
        expected_sql = """
        SELECT id, title, ts_rank(tsv, query) AS rank
        FROM posts
        WHERE published = $pub
        ORDER BY rank DESC
        """

        query = (
            ref("posts")
            .select(
                col("id"),
                col("title"),
                raw.expr("ts_rank({tsv}, {q})", tsv=col("tsv"), q=col("query")).alias("rank"),
            )
            .where(col("published") == param("pub", True))
            .order_by(col("rank").desc())
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"pub": True}

    def it_uses_raw_expr_with_alias() -> None:
        """Test aliasing raw expressions."""
        expected_sql = """
        SELECT GREATEST(x, y, $default) AS max_val
        FROM data
        """

        query = ref("data").select(
            raw.expr("GREATEST({a}, {b}, {c})", a=col("x"), b=col("y"), c=param("default", 0)).alias("max_val")
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"default": 0}

    def it_uses_raw_expr_in_aggregation() -> None:
        """Test raw expressions in GROUP BY context."""
        expected_sql = """
        SELECT region, percentile_cont($pct) WITHIN GROUP (ORDER BY amount) AS p95
        FROM sales
        GROUP BY region
        """

        query = (
            ref("sales")
            .select(
                col("region"),
                raw.expr(
                    "percentile_cont({p}) WITHIN GROUP (ORDER BY {amt})", p=param("pct", 0.95), amt=col("amount")
                ).alias("p95"),
            )
            .group_by(col("region"))
        )

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"pct": 0.95}


def describe_raw_in_subqueries() -> None:
    """Test raw expressions in subqueries."""

    def it_uses_raw_expr_in_subquery() -> None:
        """Test raw.expr() in scalar subquery."""
        expected_sql = """
        SELECT id
        FROM posts
        WHERE id = (SELECT MAX(id) AS max_id FROM posts)
        """

        subquery = ref("posts").select(raw.expr("MAX({id})", id=col("id")).alias("max_id"))
        query = ref("posts").select(col("id")).where(col("id") == subquery)

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_uses_raw_source_in_cte() -> None:
        """Test raw.rowset() in CTE."""
        expected_sql = """
        WITH series AS (SELECT num FROM generate_series(1, $max) AS t(num))
        SELECT num
        FROM series
        """

        series_cte = cte(
            "series", raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10)).select(col("num"))
        )
        query = series_cte.select(col("num"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {"max": 10}


def describe_raw_safety() -> None:
    """Test raw SQL safety considerations."""

    def it_maintains_parameter_safety_when_using_param() -> None:
        """Test that param() maintains safety even in raw SQL."""
        expected_sql = """
        SELECT custom_func($val)
        FROM data
        """

        # Good: Using param() for user input
        user_input = "malicious'; DROP TABLE users; --"
        expr = raw.expr("custom_func({input})", input=param("val", user_input))
        query = ref("data").select(expr)

        result = render(query)
        assert result.query == sql(expected_sql)
        # Parameter should be in params dict, not in query string
        assert "DROP TABLE" not in result.query
        assert result.params == {"val": user_input}

    def it_allows_complex_expressions() -> None:
        """Test that complex SQL expressions work."""
        expected_sql = """
        SELECT CASE WHEN x > y THEN x ELSE y END AS max_val
        FROM data
        """

        expr = raw.expr(
            "CASE WHEN {a} > {b} THEN {a} ELSE {b} END",
            a=col("x"),
            b=col("y"),
        )
        query = ref("data").select(expr.alias("max_val"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}


def describe_raw_edge_cases() -> None:
    """Test edge cases and error conditions."""

    def it_handles_empty_template() -> None:
        """Test rendering empty template."""
        expected_sql = """
        SELECT  AS empty
        FROM data
        """

        expr = raw.expr("")
        query = ref("data").select(expr.alias("empty"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_handles_no_placeholders() -> None:
        """Test template without placeholders."""
        expected_sql = """
        SELECT NOW() AS now
        FROM data
        """

        expr = raw.expr("NOW()")
        query = ref("data").select(expr.alias("now"))

        result = render(query)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_handles_placeholder_order_independence() -> None:
        """Test that kwarg order doesn't matter."""
        expected_sql = """
        SELECT x + y
        FROM data
        """

        # Order of kwargs shouldn't matter
        expr1 = raw.expr("{a} + {b}", a=col("x"), b=col("y"))
        expr2 = raw.expr("{a} + {b}", b=col("y"), a=col("x"))

        result1 = render(ref("data").select(expr1))
        result2 = render(ref("data").select(expr2))

        assert result1.query == sql(expected_sql)
        assert result2.query == sql(expected_sql)
        assert result1.params == {}
        assert result2.params == {}


def describe_raw_func() -> None:
    """Test raw.func() convenience method."""

    def it_renders_zero_args() -> None:
        """Test function with zero arguments."""
        expected_sql = """SELECT RANDOM() FROM data"""

        q = ref("data").select(raw.func("random"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_one_arg() -> None:
        """Test function with one argument."""
        expected_sql = """SELECT UPPER(name) FROM users"""

        q = ref("users").select(raw.func("upper", col("name")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_multiple_args() -> None:
        """Test function with multiple arguments."""
        expected_sql = """SELECT CUSTOM_FUNC(id, $val) FROM data"""

        q = ref("data").select(raw.func("custom_func", col("id"), param("val", 42)))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"val": 42}

    def it_renders_with_alias() -> None:
        """Test function with alias."""
        expected_sql = """SELECT CUSTOM_HASH(email) AS hash FROM users"""

        q = ref("users").select(raw.func("custom_hash", col("email")).alias("hash"))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}

    def it_renders_in_where() -> None:
        """Test function in WHERE clause."""
        expected_sql = """SELECT id FROM users WHERE CUSTOM_CHECK(status, $active)"""

        q = ref("users").select(col("id")).where(raw.func("custom_check", col("status"), param("active", True)))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {"active": True}

    def it_uppercases_function_name() -> None:
        """Test that function names are uppercased."""
        expected_sql = """SELECT MY_FUNC(x) FROM data"""

        q = ref("data").select(raw.func("my_func", col("x")))
        result = render(q)
        assert result.query == sql(expected_sql)
        assert result.params == {}
