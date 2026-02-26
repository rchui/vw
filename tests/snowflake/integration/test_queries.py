"""Integration tests for Snowflake query building via the public API."""

import pytest

from tests.utils import sql
from vw.core.exceptions import RenderError
from vw.snowflake import (
    F,
    col,
    cte,
    cube,
    exists,
    grouping_sets,
    lit,
    param,
    ref,
    render,
    rollup,
    when,
)


def describe_basic_select() -> None:
    def it_builds_simple_select() -> None:
        expected = "SELECT id, name, email FROM users"
        q = ref("users").select(col("id"), col("name"), col("email"))
        result = render(q)
        assert result.query == expected
        assert result.params == {}

    def it_builds_select_with_where_and_param() -> None:
        q = ref("users").select(col("id"), col("name")).where(col("age") >= param("min_age", 18))
        result = render(q)
        assert result.query == "SELECT id, name FROM users WHERE age >= %(min_age)s"
        assert result.params == {"min_age": 18}

    def it_builds_select_with_multiple_where_conditions() -> None:
        q = ref("users").select(col("id")).where(col("active"), col("verified"))
        result = render(q)
        assert result.query == "SELECT id FROM users WHERE active AND verified"
        assert result.params == {}

    def it_builds_select_with_pagination() -> None:
        expected = """
            SELECT id, name
            FROM users
            ORDER BY id
            LIMIT 10 OFFSET 20
        """
        q = ref("users").select(col("id"), col("name")).order_by(col("id")).offset(20).limit(10)
        result = render(q)
        assert result.query == sql(expected)
        assert result.params == {}

    def it_builds_select_distinct() -> None:
        q = ref("products").select(col("category")).distinct()
        result = render(q)
        assert result.query == "SELECT DISTINCT category FROM products"
        assert result.params == {}


def describe_joins() -> None:
    def it_builds_inner_join() -> None:
        u = ref("users").alias("u")
        o = ref("orders").alias("o")
        q = u.select(col("u.name"), col("o.total")).join.inner(o, on=[col("u.id") == col("o.user_id")])
        result = render(q)
        assert result.query == "SELECT u.name, o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)"
        assert result.params == {}

    def it_builds_left_join() -> None:
        u = ref("users").alias("u")
        o = ref("orders").alias("o")
        q = u.select(col("u.name"), col("o.total")).join.left(o, on=[col("u.id") == col("o.user_id")])
        result = render(q)
        assert result.query == "SELECT u.name, o.total FROM users AS u LEFT JOIN orders AS o ON (u.id = o.user_id)"
        assert result.params == {}

    def it_builds_right_join() -> None:
        u = ref("users").alias("u")
        o = ref("orders").alias("o")
        q = u.select(col("u.name"), col("o.total")).join.right(o, on=[col("u.id") == col("o.user_id")])
        result = render(q)
        assert result.query == "SELECT u.name, o.total FROM users AS u RIGHT JOIN orders AS o ON (u.id = o.user_id)"
        assert result.params == {}

    def it_builds_full_outer_join() -> None:
        a = ref("a")
        b = ref("b")
        q = a.select(col("a.x"), col("b.y")).join.full_outer(b, on=[col("a.id") == col("b.id")])
        result = render(q)
        assert result.query == "SELECT a.x, b.y FROM a FULL JOIN b ON (a.id = b.id)"
        assert result.params == {}

    def it_builds_cross_join() -> None:
        a = ref("a")
        b = ref("b")
        q = a.select(col("a.x"), col("b.y")).join.cross(b)
        result = render(q)
        assert result.query == "SELECT a.x, b.y FROM a CROSS JOIN b"
        assert result.params == {}


def describe_aggregation() -> None:
    def it_builds_group_by() -> None:
        q = ref("orders").select(col("user_id"), F.count().alias("n")).group_by(col("user_id"))
        result = render(q)
        assert result.query == "SELECT user_id, COUNT(*) AS n FROM orders GROUP BY user_id"
        assert result.params == {}

    def it_builds_group_by_with_having() -> None:
        q = (
            ref("orders")
            .select(col("user_id"), F.sum(col("amount")).alias("total"))
            .group_by(col("user_id"))
            .having(F.sum(col("amount")) > param("min_total", 100))
        )
        result = render(q)
        assert result.query == (
            "SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING SUM(amount) > %(min_total)s"
        )
        assert result.params == {"min_total": 100}

    def it_builds_aggregates() -> None:
        q = ref("t").select(
            F.count().alias("cnt"),
            F.sum(col("v")).alias("total"),
            F.avg(col("v")).alias("avg"),
            F.min(col("v")).alias("mn"),
            F.max(col("v")).alias("mx"),
        )
        result = render(q)
        assert (
            result.query == "SELECT COUNT(*) AS cnt, SUM(v) AS total, AVG(v) AS avg, MIN(v) AS mn, MAX(v) AS mx FROM t"
        )
        assert result.params == {}


def describe_window_functions() -> None:
    def it_builds_row_number() -> None:
        q = ref("employees").select(
            col("name"),
            F.row_number().over(partition_by=[col("dept")], order_by=[col("salary").desc()]).alias("rn"),
        )
        result = render(q)
        assert result.query == (
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees"
        )
        assert result.params == {}

    def it_builds_running_sum_with_frame() -> None:
        from vw.snowflake import UNBOUNDED_PRECEDING

        q = ref("sales").select(
            col("date"),
            col("amount"),
            F.sum(col("amount"))
            .over(order_by=[col("date")])
            .rows_between(UNBOUNDED_PRECEDING, None)
            .alias("running_total"),
        )
        result = render(q)
        assert result.query == (
            "SELECT date, amount, SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM sales"
        )
        assert result.params == {}

    def it_builds_lag() -> None:
        q = ref("t").select(
            col("id"),
            F.lag(col("value"), 2).over(order_by=[col("id")]).alias("prev"),
        )
        result = render(q)
        assert result.query == "SELECT id, LAG(value, 2) OVER (ORDER BY id) AS prev FROM t"
        assert result.params == {}


def describe_ctes() -> None:
    def it_builds_simple_cte() -> None:
        active = cte("active", ref("users").select(col("id"), col("name")).where(col("active")))
        q = active.select(col("id"), col("name"))
        result = render(q)
        assert result.query == "WITH active AS (SELECT id, name FROM users WHERE active) SELECT id, name FROM active"
        assert result.params == {}

    def it_builds_multiple_ctes() -> None:
        active = cte("active", ref("users").select(col("id")).where(col("active")))
        with_orders = cte(
            "with_orders",
            active.join.inner(ref("orders").alias("o"), on=[col("active.id") == col("o.user_id")]).select(
                col("active.id")
            ),
        )
        q = with_orders.select(col("id"))
        result = render(q)
        assert "WITH" in result.query
        assert "active AS" in result.query
        assert "with_orders AS" in result.query

    def it_builds_recursive_cte() -> None:
        anchor = ref("categories").select(col("id"), col("name")).where(col("parent_id").is_null())
        tree = cte("tree", anchor, recursive=True)
        q = tree.select(col("id"), col("name"))
        result = render(q)
        assert result.query.startswith("WITH RECURSIVE tree AS")


def describe_set_operations() -> None:
    def it_builds_union_distinct() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a | b)
        assert result.query == "(SELECT id FROM a) UNION (SELECT id FROM b)"
        assert result.params == {}

    def it_builds_union_all() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a + b)
        assert result.query == "(SELECT id FROM a) UNION ALL (SELECT id FROM b)"
        assert result.params == {}

    def it_builds_intersect() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a & b)
        assert result.query == "(SELECT id FROM a) INTERSECT (SELECT id FROM b)"
        assert result.params == {}

    def it_builds_except() -> None:
        a = ref("a").select(col("id"))
        b = ref("b").select(col("id"))
        result = render(a - b)
        assert result.query == "(SELECT id FROM a) EXCEPT (SELECT id FROM b)"
        assert result.params == {}


def describe_subqueries() -> None:
    def it_builds_scalar_subquery() -> None:
        sub = ref("orders").select(F.max(col("amount")))
        q = ref("orders").select(col("id")).where(col("amount") == sub)
        result = render(q)
        assert result.query == "SELECT id FROM orders WHERE amount = (SELECT MAX(amount) FROM orders)"
        assert result.params == {}

    def it_builds_join_with_subquery() -> None:
        sub = (
            ref("orders")
            .select(col("user_id"), F.sum(col("amount")).alias("total"))
            .group_by(col("user_id"))
            .alias("totals")
        )
        q = (
            ref("users")
            .alias("u")
            .select(col("u.id"), col("totals.total"))
            .join.inner(sub, on=[col("u.id") == col("totals.user_id")])
        )
        result = render(q)
        assert "(SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS totals" in result.query
        assert result.params == {}

    def it_builds_exists_subquery() -> None:
        users = ref("users")
        orders = ref("orders")
        q = users.select(col("id")).where(exists(orders.where(col("orders.user_id") == col("users.id"))))
        result = render(q)
        assert result.query == "SELECT id FROM users WHERE EXISTS (FROM orders WHERE orders.user_id = users.id)"
        assert result.params == {}


def describe_case_when() -> None:
    def it_builds_case_when_otherwise() -> None:
        q = ref("users").select(
            when(col("age") >= param("adult_age", 18)).then(lit("adult")).otherwise(lit("minor")).alias("category")
        )
        result = render(q)
        assert (
            result.query == "SELECT CASE WHEN age >= %(adult_age)s THEN 'adult' ELSE 'minor' END AS category FROM users"
        )
        assert result.params == {"adult_age": 18}

    def it_builds_case_when_no_else() -> None:
        q = ref("t").select(when(col("status") == lit("active")).then(lit(1)).end().alias("is_active"))
        result = render(q)
        assert result.query == "SELECT CASE WHEN status = 'active' THEN 1 END AS is_active FROM t"
        assert result.params == {}

    def it_builds_multi_when() -> None:
        q = ref("t").select(
            when(col("score") >= lit(90))
            .then(lit("A"))
            .when(col("score") >= lit(80))
            .then(lit("B"))
            .otherwise(lit("C"))
            .alias("grade")
        )
        result = render(q)
        assert (
            result.query
            == "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM t"
        )
        assert result.params == {}


def describe_grouping_sets() -> None:
    def it_builds_rollup() -> None:
        q = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(rollup(col("region"), col("product")))
        )
        result = render(q)
        assert (
            result.query == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region, product)"
        )
        assert result.params == {}

    def it_builds_cube() -> None:
        q = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(cube(col("region"), col("product")))
        )
        result = render(q)
        assert result.query == "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE (region, product)"
        assert result.params == {}

    def it_builds_grouping_sets() -> None:
        q = (
            ref("sales")
            .select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
            .group_by(grouping_sets((col("region"), col("product")), (col("region"),), ()))
        )
        result = render(q)
        assert result.query == (
            "SELECT region, product, SUM(amount) AS total FROM sales "
            "GROUP BY GROUPING SETS ((region, product), (region), ())"
        )
        assert result.params == {}


def describe_distinct() -> None:
    def it_builds_select_distinct() -> None:
        q = ref("products").select(col("category")).distinct()
        result = render(q)
        assert result.query == "SELECT DISTINCT category FROM products"
        assert result.params == {}

    def it_raises_for_distinct_on() -> None:
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


def describe_limit_offset_fetch() -> None:
    def it_builds_limit() -> None:
        q = ref("t").select(col("id")).limit(5)
        result = render(q)
        assert result.query == "SELECT id FROM t LIMIT 5"
        assert result.params == {}

    def it_builds_limit_offset() -> None:
        q = ref("t").select(col("id")).limit(10).offset(20)
        result = render(q)
        assert result.query == "SELECT id FROM t LIMIT 10 OFFSET 20"
        assert result.params == {}

    def it_builds_fetch_first() -> None:
        q = ref("t").select(col("id")).fetch(5)
        result = render(q)
        assert result.query == "SELECT id FROM t FETCH FIRST 5 ROWS ONLY"
        assert result.params == {}


def describe_raw_api() -> None:
    def it_uses_raw_expr() -> None:
        from vw.snowflake import raw

        q = ref("t").select(raw.expr("TO_DATE(created_at)").alias("day"))
        result = render(q)
        assert result.query == "SELECT TO_DATE(created_at) AS day FROM t"
        assert result.params == {}

    def it_uses_raw_expr_with_params() -> None:
        from vw.snowflake import raw

        q = ref("t").where(raw.expr("{x} = {y}", x=col("id"), y=param("uid", 42))).select(col("name"))
        result = render(q)
        assert result.query == "SELECT name FROM t WHERE id = %(uid)s"
        assert result.params == {"uid": 42}

    def it_uses_raw_rowset() -> None:
        from vw.snowflake import raw

        sub = raw.rowset("(SELECT * FROM generate_range(1, {n})) AS gs(v)", n=param("n", 5))
        q = ref("t").select(col("t.x"), col("gs.v")).join.inner(sub, on=[col("t.id") == col("gs.v")])
        result = render(q)
        assert "%(n)s" in result.query
        assert result.params == {"n": 5}


def describe_type_casting() -> None:
    """Snowflake uses :: cast syntax."""

    def it_casts_with_double_colon() -> None:
        from vw.snowflake.types import INTEGER

        q = ref("t").select(col("price").cast(INTEGER()))
        result = render(q)
        assert result.query == "SELECT price::INTEGER FROM t"
        assert result.params == {}

    def it_casts_to_variant() -> None:
        from vw.snowflake.types import VARIANT

        q = ref("t").select(col("payload").cast(VARIANT()))
        result = render(q)
        assert result.query == "SELECT payload::VARIANT FROM t"
        assert result.params == {}

    def it_casts_to_number() -> None:
        from vw.snowflake.types import NUMBER

        q = ref("t").select(col("price").cast(NUMBER(10, 2)))
        result = render(q)
        assert result.query == "SELECT price::NUMBER(10, 2) FROM t"
        assert result.params == {}

    def it_casts_to_timestamp_ntz() -> None:
        from vw.snowflake.types import TIMESTAMP_NTZ

        q = ref("t").select(col("ts").cast(TIMESTAMP_NTZ()))
        result = render(q)
        assert result.query == "SELECT ts::TIMESTAMP_NTZ FROM t"
        assert result.params == {}
