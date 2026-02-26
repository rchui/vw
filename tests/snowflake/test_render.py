"""Unit tests for Snowflake-specific rendering."""

import pytest

from vw.core.exceptions import RenderError
from vw.core.render import SQL
from vw.snowflake import F, col, cube, grouping_sets, interval, lit, param, ref, render, rollup


def describe_render_column() -> None:
    """Tests for column rendering."""

    def it_renders_bare_column() -> None:
        result = render(ref("t").select(col("id")))
        assert result == SQL(query="SELECT id FROM t", params={})

    def it_renders_column_with_alias() -> None:
        result = render(ref("t").select(col("id").alias("user_id")))
        assert result == SQL(query="SELECT id AS user_id FROM t", params={})

    def it_renders_qualified_column() -> None:
        result = render(ref("t").select(col("t.id")))
        assert result == SQL(query="SELECT t.id FROM t", params={})


def describe_render_parameter() -> None:
    """Tests for parameter rendering — Snowflake uses %(name)s style."""

    def it_renders_pyformat_placeholder() -> None:
        result = render(ref("users").select(col("id")).where(col("age") >= param("min_age", 18)))
        assert result == SQL(query="SELECT id FROM users WHERE age >= %(min_age)s", params={"min_age": 18})

    def it_renders_string_parameter() -> None:
        result = render(ref("users").select(col("id")).where(col("status") == param("status", "active")))
        assert result == SQL(query="SELECT id FROM users WHERE status = %(status)s", params={"status": "active"})

    def it_renders_multiple_parameters() -> None:
        result = render(
            ref("users")
            .select(col("id"))
            .where((col("age") >= param("min_age", 18)) & (col("age") <= param("max_age", 65)))
        )
        assert result == SQL(
            query="SELECT id FROM users WHERE (age >= %(min_age)s) AND (age <= %(max_age)s)",
            params={"min_age": 18, "max_age": 65},
        )


def describe_render_literal() -> None:
    """Tests for literal rendering."""

    def it_renders_string_literal() -> None:
        result = render(ref("t").select(lit("hello").alias("v")))
        assert result == SQL(query="SELECT 'hello' AS v FROM t", params={})

    def it_renders_integer_literal() -> None:
        result = render(ref("t").select(lit(42).alias("n")))
        assert result == SQL(query="SELECT 42 AS n FROM t", params={})

    def it_renders_float_literal() -> None:
        result = render(ref("t").select(lit(3.14).alias("pi")))
        assert result == SQL(query="SELECT 3.14 AS pi FROM t", params={})

    def it_renders_none_as_null() -> None:
        result = render(ref("t").select(lit(None).alias("v")))
        assert result == SQL(query="SELECT NULL AS v FROM t", params={})

    def it_renders_true_literal() -> None:
        result = render(ref("t").select(lit(True).alias("flag")))
        assert result == SQL(query="SELECT TRUE AS flag FROM t", params={})

    def it_renders_false_literal() -> None:
        result = render(ref("t").select(lit(False).alias("flag")))
        assert result == SQL(query="SELECT FALSE AS flag FROM t", params={})

    def it_escapes_single_quotes_in_string() -> None:
        result = render(ref("t").select(lit("it's").alias("v")))
        assert result == SQL(query="SELECT 'it''s' AS v FROM t", params={})


def describe_render_cast() -> None:
    """Tests for type cast rendering — Snowflake uses :: syntax."""

    def it_renders_cast_with_double_colon() -> None:
        from vw.snowflake.types import INTEGER

        result = render(ref("t").select(col("id").cast(INTEGER())))
        assert result == SQL(query="SELECT id::INTEGER FROM t", params={})

    def it_renders_cast_varchar() -> None:
        from vw.snowflake.types import VARCHAR

        result = render(ref("t").select(col("amount").cast(VARCHAR())))
        assert result == SQL(query="SELECT amount::VARCHAR FROM t", params={})

    def it_renders_snowflake_variant_cast() -> None:
        from vw.snowflake.types import VARIANT

        result = render(ref("t").select(col("data").cast(VARIANT())))
        assert result == SQL(query="SELECT data::VARIANT FROM t", params={})

    def it_renders_snowflake_number_cast() -> None:
        from vw.snowflake.types import NUMBER

        result = render(ref("t").select(col("price").cast(NUMBER(10, 2))))
        assert result == SQL(query="SELECT price::NUMBER(10, 2) FROM t", params={})

    def it_renders_snowflake_timestamp_ntz_cast() -> None:
        from vw.snowflake.types import TIMESTAMP_NTZ

        result = render(ref("t").select(col("ts").cast(TIMESTAMP_NTZ())))
        assert result == SQL(query="SELECT ts::TIMESTAMP_NTZ FROM t", params={})


def describe_render_function() -> None:
    """Tests for function rendering."""

    def it_renders_count_star() -> None:
        result = render(ref("t").select(F.count()))
        assert result == SQL(query="SELECT COUNT(*) FROM t", params={})

    def it_renders_sum() -> None:
        result = render(ref("t").select(F.sum(col("amount")).alias("total")))
        assert result == SQL(query="SELECT SUM(amount) AS total FROM t", params={})

    def it_renders_avg() -> None:
        result = render(ref("t").select(F.avg(col("score")).alias("avg_score")))
        assert result == SQL(query="SELECT AVG(score) AS avg_score FROM t", params={})

    def it_renders_count_distinct() -> None:
        result = render(ref("t").select(F.count(col("id"), distinct=True).alias("unique")))
        assert result == SQL(query="SELECT COUNT(DISTINCT id) AS unique FROM t", params={})

    def it_renders_current_timestamp() -> None:
        result = render(ref("t").select(F.current_timestamp().alias("now")))
        assert result == SQL(query="SELECT CURRENT_TIMESTAMP AS now FROM t", params={})

    def it_renders_current_date() -> None:
        result = render(ref("t").select(F.current_date().alias("today")))
        assert result == SQL(query="SELECT CURRENT_DATE AS today FROM t", params={})


def describe_render_window_function() -> None:
    """Tests for window function rendering."""

    def it_renders_row_number() -> None:
        result = render(
            ref("t").select(
                F.row_number().over(partition_by=[col("dept")], order_by=[col("salary").desc()]).alias("rn")
            )
        )
        assert result == SQL(
            query="SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM t",
            params={},
        )

    def it_renders_rank() -> None:
        result = render(
            ref("t").select(F.rank().over(partition_by=[col("dept")], order_by=[col("score").desc()]).alias("r"))
        )
        assert result == SQL(
            query="SELECT RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS r FROM t",
            params={},
        )

    def it_renders_window_with_frame() -> None:
        from vw.snowflake import UNBOUNDED_PRECEDING

        result = render(
            ref("t").select(
                F.sum(col("amount"))
                .over(partition_by=[col("dept")], order_by=[col("date")])
                .rows_between(UNBOUNDED_PRECEDING, None)
                .alias("running_total")
            )
        )
        assert result == SQL(
            query="SELECT SUM(amount) OVER (PARTITION BY dept ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM t",
            params={},
        )


def describe_render_statement() -> None:
    """Tests for full statement rendering."""

    def it_renders_basic_select() -> None:
        result = render(ref("users").select(col("id"), col("name")))
        assert result == SQL(query="SELECT id, name FROM users", params={})

    def it_renders_where_clause() -> None:
        result = render(ref("users").select(col("id")).where(col("active")))
        assert result == SQL(query="SELECT id FROM users WHERE active", params={})

    def it_renders_group_by() -> None:
        result = render(ref("orders").select(col("user_id"), F.count().alias("n")).group_by(col("user_id")))
        assert result == SQL(query="SELECT user_id, COUNT(*) AS n FROM orders GROUP BY user_id", params={})

    def it_renders_having() -> None:
        result = render(
            ref("orders")
            .select(col("user_id"), F.count().alias("n"))
            .group_by(col("user_id"))
            .having(F.count() > lit(5))
        )
        assert result == SQL(
            query="SELECT user_id, COUNT(*) AS n FROM orders GROUP BY user_id HAVING COUNT(*) > 5",
            params={},
        )

    def it_renders_order_by() -> None:
        result = render(ref("users").select(col("name")).order_by(col("name").asc()))
        assert result == SQL(query="SELECT name FROM users ORDER BY name ASC", params={})

    def it_renders_limit_offset() -> None:
        result = render(ref("users").select(col("id")).limit(10).offset(20))
        assert result == SQL(query="SELECT id FROM users LIMIT 10 OFFSET 20", params={})

    def it_renders_select_distinct() -> None:
        result = render(ref("products").select(col("category")).distinct())
        assert result == SQL(query="SELECT DISTINCT category FROM products", params={})

    def it_raises_for_distinct_on() -> None:
        """Snowflake does not support DISTINCT ON."""
        from vw.core.base import Factories
        from vw.core.states import Column, Distinct, Reference, Statement
        from vw.snowflake.base import Expression
        from vw.snowflake.base import RowSet as SnowflakeRowSet

        stmt = Statement(
            source=Reference(name="t"),
            columns=(Column(name="id"),),
            distinct=Distinct(on=(Column(name="name"),)),
        )
        rs = SnowflakeRowSet(state=stmt, factories=Factories(expr=Expression, rowset=SnowflakeRowSet))
        with pytest.raises(RenderError, match="DISTINCT ON"):
            render(rs)

    def it_raises_for_unsupported_modifier() -> None:
        """FOR UPDATE / row-level locking is not supported in Snowflake."""
        # Manually attach a RowLock modifier to a Snowflake RowSet to simulate
        # unsupported modifier usage
        from dataclasses import replace

        from vw.core.base import Factories
        from vw.core.states import Statement
        from vw.postgres.states import RowLock
        from vw.snowflake.base import Expression
        from vw.snowflake.base import RowSet as SnowflakeRowSet
        from vw.snowflake.render import render as sf_render

        base_rs = ref("t").select(col("id"))
        lock_state = RowLock(strength="UPDATE")
        stmt = base_rs.state
        assert isinstance(stmt, Statement)
        modified_stmt = replace(stmt, modifiers=(lock_state,))
        rs = SnowflakeRowSet(
            state=modified_stmt,
            factories=Factories(expr=Expression, rowset=SnowflakeRowSet),
        )
        with pytest.raises(RenderError):
            sf_render(rs)


def describe_render_join() -> None:
    """Tests for JOIN rendering."""

    def it_renders_inner_join() -> None:
        u = ref("users").alias("u")
        o = ref("orders").alias("o")
        result = render(u.select(col("u.name")).join.inner(o, on=[col("u.id") == col("o.user_id")]))
        assert result == SQL(
            query="SELECT u.name FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)",
            params={},
        )

    def it_renders_left_join() -> None:
        u = ref("users").alias("u")
        o = ref("orders").alias("o")
        result = render(u.select(col("u.name")).join.left(o, on=[col("u.id") == col("o.user_id")]))
        assert result == SQL(
            query="SELECT u.name FROM users AS u LEFT JOIN orders AS o ON (u.id = o.user_id)",
            params={},
        )

    def it_renders_cross_join() -> None:
        a = ref("a")
        b = ref("b")
        result = render(a.select(col("a.x"), col("b.y")).join.cross(b))
        assert result == SQL(query="SELECT a.x, b.y FROM a CROSS JOIN b", params={})

    def it_renders_lateral_join() -> None:
        users = ref("users").alias("u")
        sub = ref("orders").alias("o")
        result = render(users.select(col("u.name")).join.inner(sub, on=[col("u.id") == col("o.user_id")], lateral=True))
        assert result == SQL(
            query="SELECT u.name FROM users AS u INNER JOIN LATERAL orders AS o ON (u.id = o.user_id)",
            params={},
        )


def describe_render_cte() -> None:
    """Tests for CTE rendering."""

    def it_renders_simple_cte() -> None:
        from vw.snowflake import cte

        active = cte("active", ref("users").select(col("id"), col("name")).where(col("active")))
        result = render(active.select(col("id")))
        assert result == SQL(
            query="WITH active AS (SELECT id, name FROM users WHERE active) SELECT id FROM active",
            params={},
        )

    def it_renders_recursive_cte() -> None:
        from vw.snowflake import cte

        anchor = ref("categories").select(col("id"), col("name"), col("parent_id")).where(col("parent_id").is_null())
        tree = cte("tree", anchor, recursive=True)
        result = render(tree.select(col("id"), col("name")))
        assert result.query.startswith("WITH RECURSIVE tree AS")


def describe_render_set_operation() -> None:
    """Tests for set operation rendering."""

    def it_renders_union_distinct() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a | b)
        assert result == SQL(query="(SELECT id FROM a) UNION (SELECT id FROM b)", params={})

    def it_renders_union_all() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a + b)
        assert result == SQL(query="(SELECT id FROM a) UNION ALL (SELECT id FROM b)", params={})

    def it_renders_intersect() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a & b)
        assert result == SQL(query="(SELECT id FROM a) INTERSECT (SELECT id FROM b)", params={})

    def it_renders_except() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a - b)
        assert result == SQL(query="(SELECT id FROM a) EXCEPT (SELECT id FROM b)", params={})


def describe_render_exists() -> None:
    """Tests for EXISTS subquery rendering."""

    def it_renders_exists() -> None:
        from vw.snowflake import exists

        users = ref("users")
        orders = ref("orders")
        result = render(users.select(col("id")).where(exists(orders.where(col("orders.user_id") == col("users.id")))))
        assert result == SQL(
            query="SELECT id FROM users WHERE EXISTS (FROM orders WHERE orders.user_id = users.id)",
            params={},
        )


def describe_render_raw_expr() -> None:
    """Tests for raw expression escape hatch rendering."""

    def it_renders_raw_expr_with_no_params() -> None:
        from vw.snowflake import raw

        result = render(ref("t").select(raw.expr("custom_func()")))
        assert result == SQL(query="SELECT custom_func() FROM t", params={})

    def it_renders_raw_expr_with_params() -> None:
        from vw.snowflake import raw

        result = render(ref("t").select(col("id")).where(raw.expr("{x} @> {y}", x=col("tags"), y=param("tag", "py"))))
        assert result == SQL(query="SELECT id FROM t WHERE tags @> %(tag)s", params={"tag": "py"})


def describe_render_raw_source() -> None:
    """Tests for raw source escape hatch rendering."""

    def it_renders_raw_source_in_join() -> None:
        from vw.snowflake import raw

        result = render(
            ref("t")
            .select(col("id"))
            .join.inner(raw.rowset("lateral_func({n}) AS r", n=param("n", 5)), on=[col("t.id") == col("r.id")])
        )
        assert result == SQL(
            query="SELECT id FROM t INNER JOIN lateral_func(%(n)s) AS r ON (t.id = r.id)",
            params={"n": 5},
        )


def describe_render_frame_clause() -> None:
    """Tests for window frame clause rendering."""

    def it_renders_rows_between_unbounded() -> None:
        from vw.snowflake import UNBOUNDED_FOLLOWING, UNBOUNDED_PRECEDING

        result = render(
            ref("t").select(
                F.sum(col("v")).over().rows_between(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING).alias("total")
            )
        )
        assert result == SQL(
            query="SELECT SUM(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total FROM t",
            params={},
        )

    def it_renders_rows_between_preceding_and_current() -> None:
        from vw.snowflake import UNBOUNDED_PRECEDING

        result = render(
            ref("t").select(F.sum(col("v")).over().rows_between(UNBOUNDED_PRECEDING, None).alias("running"))
        )
        assert result == SQL(
            query="SELECT SUM(v) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM t",
            params={},
        )


def describe_render_grouping_constructs() -> None:
    """Tests for ROLLUP, CUBE, and GROUPING SETS rendering."""

    def it_renders_rollup() -> None:
        result = render(
            ref("sales")
            .select(col("region"), F.sum(col("amount")).alias("total"))
            .group_by(rollup(col("region"), col("product")))
        )
        assert result == SQL(
            query="SELECT region, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region, product)",
            params={},
        )

    def it_renders_cube() -> None:
        result = render(
            ref("sales")
            .select(col("region"), F.sum(col("amount")).alias("total"))
            .group_by(cube(col("region"), col("product")))
        )
        assert result == SQL(
            query="SELECT region, SUM(amount) AS total FROM sales GROUP BY CUBE (region, product)",
            params={},
        )

    def it_renders_grouping_sets() -> None:
        result = render(
            ref("sales")
            .select(col("region"), F.sum(col("amount")).alias("total"))
            .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
        )
        assert result == SQL(
            query="SELECT region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region, product), (region), ())",
            params={},
        )


def describe_render_interval() -> None:
    """Tests for INTERVAL literal rendering."""

    def it_renders_interval_day() -> None:
        result = render(ref("t").select(interval(1, "day").alias("d")))
        assert result == SQL(query="SELECT INTERVAL '1 day' AS d FROM t", params={})

    def it_renders_interval_in_expression() -> None:
        result = render(ref("events").select((col("created_at") + interval(7, "day")).alias("next_week")))
        assert result == SQL(query="SELECT created_at + INTERVAL '7 day' AS next_week FROM events", params={})


def describe_render_ilike() -> None:
    """Tests for ILIKE (case-insensitive LIKE) rendering — supported in Snowflake."""

    def it_renders_ilike() -> None:
        result = render(ref("users").select(col("name")).where(col("email").ilike(param("p", "%@gmail.com"))))
        assert result == SQL(
            query="SELECT name FROM users WHERE email ILIKE %(p)s",
            params={"p": "%@gmail.com"},
        )

    def it_renders_not_ilike() -> None:
        result = render(ref("users").select(col("name")).where(col("email").not_ilike(param("p", "%@spam.com"))))
        assert result == SQL(
            query="SELECT name FROM users WHERE email NOT ILIKE %(p)s",
            params={"p": "%@spam.com"},
        )
