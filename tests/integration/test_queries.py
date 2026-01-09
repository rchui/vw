"""Integration tests for SQL query clauses: SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT."""

import vw
from tests.utils import sql


def describe_basic_select() -> None:
    """Tests for basic SELECT statements."""

    def it_generates_select_star(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
        """
        result = vw.Source(name="users").select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_select_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id FROM products
        """
        result = vw.Source(name="products").select(vw.col("id")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_select_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, customer_id, total FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.col("customer_id"), vw.col("total"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_where():
    """Tests for WHERE clause."""

    def it_generates_where_with_single_condition(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (age >= 18)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("age") >= vw.col("18"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (age >= 18) AND (status = 'active')
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("age") >= vw.col("18"), vw.col("status") == vw.col("'active'"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (age >= :min_age) AND (status = :status)
        """
        min_age = vw.param("min_age", 18)
        status = vw.param("status", "active")
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("age") >= min_age, vw.col("status") == status)
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"min_age": 18, "status": "active"})

    def it_chains_multiple_where_calls(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (age >= 18) AND (status = 'active')
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("age") >= vw.col("18"))
            .where(vw.col("status") == vw.col("'active'"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_all_comparison_operators(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM products
            WHERE (price > 10)
                AND (stock >= 5)
                AND (discount < 50)
                AND (rating <= 4.5)
                AND (active = true)
                AND (deleted <> true)
        """
        result = (
            vw.Source(name="products")
            .select(vw.col("*"))
            .where(
                vw.col("price") > vw.col("10"),
                vw.col("stock") >= vw.col("5"),
                vw.col("discount") < vw.col("50"),
                vw.col("rating") <= vw.col("4.5"),
                vw.col("active") == vw.col("true"),
                vw.col("deleted") != vw.col("true"),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_logical_operators(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM employees
            WHERE ((department = 'Sales') OR (department = 'Marketing'))
                AND ((hire_date >= '2020-01-01') AND (active = true))
        """
        result = (
            vw.Source(name="employees")
            .select(vw.col("*"))
            .where(
                ((vw.col("department") == vw.col("'Sales'")) | (vw.col("department") == vw.col("'Marketing'"))),
                ((vw.col("hire_date") >= vw.col("'2020-01-01'")) & (vw.col("active") == vw.col("true"))),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_not_operator(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (NOT (deleted = true))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(~(vw.col("deleted") == vw.col("true")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_not_combined_with_and(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            WHERE ((NOT (deleted = true)) AND (status = 'active'))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(~(vw.col("deleted") == vw.col("true")) & (vw.col("status") == vw.col("'active'")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_group_by():
    """Tests for GROUP BY clause."""

    def it_generates_basic_group_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, SUM(total)
            FROM orders
            GROUP BY customer_id
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_group_by_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, status, COUNT(*)
            FROM orders
            GROUP BY customer_id, status
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("status"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"), vw.col("status"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_group_by_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, SUM(total)
            FROM orders
            WHERE (status = 'completed')
            GROUP BY customer_id
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.col("customer_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_having():
    """Tests for HAVING clause."""

    def it_generates_having_with_group_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, COUNT(*)
            FROM orders
            GROUP BY customer_id
            HAVING (COUNT(*) > 5)
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.col("5"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_having_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, COUNT(*)
            FROM orders
            GROUP BY customer_id
            HAVING (COUNT(*) > :min_orders)
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.param("min_orders", 5))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"min_orders": 5})

    def it_generates_full_query_with_all_clauses(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, COUNT(*), SUM(total)
            FROM orders
            WHERE (status = :status)
            GROUP BY customer_id
            HAVING (COUNT(*) >= :min_orders) AND (SUM(total) > :min_total)
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.param("status", "completed"))
            .group_by(vw.col("customer_id"))
            .having(
                vw.col("COUNT(*)") >= vw.param("min_orders", 3),
                vw.col("SUM(total)") > vw.param("min_total", 1000),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status": "completed", "min_orders": 3, "min_total": 1000},
        )


def describe_order_by():
    """Tests for ORDER BY clause."""

    def it_generates_basic_order_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY name ASC
        """
        result = vw.Source(name="users").select(vw.col("*")).order_by(vw.col("name").asc()).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_order_by_without_direction(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY name
        """
        result = vw.Source(name="users").select(vw.col("*")).order_by(vw.col("name")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_order_by_desc(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY created_at DESC
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("created_at").desc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_order_by_with_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY last_name ASC, first_name ASC, created_at DESC
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("last_name").asc(), vw.col("first_name").asc(), vw.col("created_at").desc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_limit():
    """Tests for LIMIT clause."""

    def it_generates_basic_limit(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users LIMIT 10
        """
        result = vw.Source(name="users").select(vw.col("*")).limit(10).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_limit_with_offset(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users LIMIT 10 OFFSET 20
        """
        result = vw.Source(name="users").select(vw.col("*")).limit(10, offset=20).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_full_query_with_limit(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, COUNT(*), SUM(total)
            FROM orders
            WHERE (status = :status)
            GROUP BY customer_id
            HAVING (COUNT(*) >= :min_orders)
            ORDER BY SUM(total) DESC
            LIMIT 10 OFFSET 5
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.param("status", "completed"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") >= vw.param("min_orders", 3))
            .order_by(vw.col("SUM(total)").desc())
            .limit(10, offset=5)
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status": "completed", "min_orders": 3},
        )

    def it_generates_sqlserver_offset_fetch() -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY id ASC OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
        """
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("id").asc())
            .limit(10, offset=20)
            .render(config=config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_sqlserver_without_offset() -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY id ASC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
        """
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        result = (
            vw.Source(name="users").select(vw.col("*")).order_by(vw.col("id").asc()).limit(10).render(config=config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_distinct():
    """Tests for DISTINCT queries."""

    def it_generates_distinct(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT name FROM users"
        stmt = vw.Source(name="users").select(vw.col("name")).distinct()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT first_name, last_name FROM users"
        stmt = vw.Source(name="users").select(vw.col("first_name"), vw.col("last_name")).distinct()
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT name FROM users WHERE (active = true)"
        stmt = vw.Source(name="users").select(vw.col("name")).distinct().where(vw.col("active") == vw.col("true"))
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_with_order_by(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT name FROM users ORDER BY name ASC"
        stmt = vw.Source(name="users").select(vw.col("name")).distinct().order_by(vw.col("name").asc())
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_on_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT ON (department) department, name, salary FROM employees"
        stmt = (
            vw.Source(name="employees")
            .select(vw.col("department"), vw.col("name"), vw.col("salary"))
            .distinct(on=[vw.col("department")])
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_on_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = "SELECT DISTINCT ON (department, region) department, region, name FROM employees"
        stmt = (
            vw.Source(name="employees")
            .select(vw.col("department"), vw.col("region"), vw.col("name"))
            .distinct(on=[vw.col("department"), vw.col("region")])
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})

    def it_generates_distinct_on_with_order_by(render_config: vw.RenderConfig) -> None:
        expected_sql = (
            "SELECT DISTINCT ON (department) department, name, salary "
            "FROM employees ORDER BY department ASC, salary DESC"
        )
        stmt = (
            vw.Source(name="employees")
            .select(vw.col("department"), vw.col("name"), vw.col("salary"))
            .distinct(on=[vw.col("department")])
            .order_by(vw.col("department").asc(), vw.col("salary").desc())
        )
        result = stmt.render(config=render_config)
        assert result == vw.RenderResult(sql=expected_sql, params={})
