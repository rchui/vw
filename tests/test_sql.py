"""Integration tests for SQL generation."""

import textwrap

import vw


def sql(query: str) -> str:
    """
    Convert a multi-line SQL string to a single-line string for comparison.

    Removes leading/trailing whitespace and collapses internal whitespace.
    """
    return " ".join(textwrap.dedent(query).strip().split())


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

    def it_generates_group_by_with_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customers.name, COUNT(*)
            FROM customers
            INNER JOIN orders ON (customers.id = orders.customer_id)
            GROUP BY customers.name
        """
        customers = vw.Source(name="customers")
        orders = vw.Source(name="orders")
        result = (
            customers.join.inner(orders, on=[customers.col("id") == orders.col("customer_id")])
            .select(customers.col("name"), vw.col("COUNT(*)"))
            .group_by(customers.col("name"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_group_by_within_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH customer_totals AS (SELECT customer_id, SUM(total) AS total FROM orders GROUP BY customer_id)
            SELECT * FROM customer_totals
        """
        customer_totals = vw.cte(
            "customer_totals",
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)").alias("total"))
            .group_by(vw.col("customer_id")),
        )
        result = customer_totals.select(vw.col("*")).render(config=render_config)
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

    def it_generates_having_with_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_orders AS (SELECT * FROM orders WHERE (status = 'active'))
            SELECT customer_id, SUM(total)
            FROM active_orders
            GROUP BY customer_id
            HAVING (SUM(total) > 500)
        """
        active_orders = vw.cte(
            "active_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = (
            active_orders.select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("SUM(total)") > vw.col("500"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_order_by():
    """Tests for ORDER BY clause."""

    def it_generates_basic_order_by(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY name ASC
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("name").asc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_order_by_without_direction(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users ORDER BY name
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("name"))
            .render(config=render_config)
        )
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

    def it_generates_full_query_with_all_clauses(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT customer_id, COUNT(*), SUM(total)
            FROM orders
            WHERE (status = :status)
            GROUP BY customer_id
            HAVING (COUNT(*) >= :min_orders)
            ORDER BY SUM(total) DESC
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.param("status", "completed"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") >= vw.param("min_orders", 3))
            .order_by(vw.col("SUM(total)").desc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status": "completed", "min_orders": 3},
        )

    def it_generates_order_by_with_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
            ORDER BY orders.total DESC, users.name ASC
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("name"), orders.col("total"))
            .order_by(orders.col("total").desc(), users.col("name").asc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_star_extensions() -> None:
    """Tests for star expression extensions."""

    def it_generates_star_replace(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * REPLACE (name AS full_name) FROM users
        """
        result = vw.Source(name="users").select(vw.col("* REPLACE (name AS full_name)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_star_exclude(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * EXCLUDE (password) FROM users
        """
        result = vw.Source(name="users").select(vw.col("* EXCLUDE (password)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_star_exclude_multiple(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * EXCLUDE (password, ssn) FROM users
        """
        result = vw.Source(name="users").select(vw.col("* EXCLUDE (password, ssn)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_method_chaining() -> None:
    """Tests for method chaining patterns."""

    def it_chains_source_select_render() -> None:
        expected_sql = """
            SELECT id, name FROM employees
        """
        result = vw.Source(name="employees").select(vw.col("id"), vw.col("name")).render(config=vw.RenderConfig())
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_allows_breaking_chain_into_steps() -> None:
        expected_sql = """
            SELECT * FROM products
        """
        source = vw.Source(name="products")
        statement = source.select(vw.col("*"))
        result = statement.render(config=vw.RenderConfig())
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_complex_expressions() -> None:
    """Tests for complex SQL expressions via escape hatch."""

    def it_generates_cast_expression(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CAST(price AS DECIMAL(10,2)) FROM sales
        """
        result = vw.Source(name="sales").select(vw.col("CAST(price AS DECIMAL(10,2))")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_mixes_simple_and_complex_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, ROUND(total, 2) AS rounded_total, status FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.col("ROUND(total, 2) AS rounded_total"), vw.col("status"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_joins():
    """Tests for JOIN operations."""

    def it_generates_inner_join_with_qualified_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_inner_join_with_selected_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(users.col("name"), orders.col("total")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_inner_join_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON (users.id = orders.user_id)
                AND (users.status = 'active')
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.inner(
            orders, on=[users.col("id") == orders.col("user_id"), users.col("status") == vw.col("'active'")]
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_multiple_joins(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.quantity, products.price
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
            INNER JOIN products ON (orders.product_id = products.id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        products = vw.Source(name="products")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        joined = joined.join.inner(products, on=[orders.col("product_id") == products.col("id")])
        result = joined.select(users.col("name"), orders.col("quantity"), products.col("price")).render(
            config=render_config
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_join_without_on_condition(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN settings
        """
        users = vw.Source(name="users")
        settings = vw.Source(name="settings")
        joined = users.join.inner(settings)
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_chains_join_and_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
        """
        result = (
            vw.Source(name="users")
            .join.inner(
                vw.Source(name="orders"),
                on=[vw.Source(name="users").col("id") == vw.Source(name="orders").col("user_id")],
            )
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_complex_logical_conditions_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON ((users.id = orders.user_id) OR (users.email = orders.contact_email))
                AND (orders.status = 'completed')
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.inner(
            orders,
            on=[
                (users.col("id") == orders.col("user_id")) | (users.col("email") == orders.col("contact_email")),
                orders.col("status") == vw.col("'completed'"),
            ],
        )
        result = joined.select(vw.col("*")).render(config=render_config)
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

    def it_generates_where_with_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
            WHERE (orders.total > 100)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(vw.col("*"))
            .where(orders.col("total") > vw.col("100"))
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


def describe_subqueries():
    """Tests for subquery support."""

    def it_generates_subquery_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN (SELECT user_id, total FROM orders) AS order_totals
        """
        users = vw.Source(name="users")
        order_totals = vw.Source(name="orders").select(vw.col("user_id"), vw.col("total")).alias("order_totals")
        result = users.join.inner(order_totals).select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_subquery_in_join_with_condition(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, ot.total
            FROM users
            INNER JOIN (SELECT user_id, total FROM orders) AS ot
                ON (users.id = ot.user_id)
        """
        users = vw.Source(name="users")
        ot = vw.Source(name="orders").select(vw.col("user_id"), vw.col("total")).alias("ot")
        result = (
            users.join.inner(ot, on=[users.col("id") == ot.col("user_id")])
            .select(users.col("name"), ot.col("total"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_nested_subqueries(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM (SELECT * FROM (SELECT id FROM users) AS inner_q) AS outer_q
        """
        inner = vw.Source(name="users").select(vw.col("id")).alias("inner_q")
        middle = vw.Statement(source=inner, columns=[vw.col("*")]).alias("outer_q")
        outer = vw.Statement(source=middle, columns=[vw.col("*")])
        result = outer.render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_subquery_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN (SELECT user_id FROM orders WHERE (status = :status)) AS active_orders
        """
        users = vw.Source(name="users")
        active_orders = (
            vw.Source(name="orders")
            .select(vw.col("user_id"))
            .where(vw.col("status") == vw.param("status", "active"))
            .alias("active_orders")
        )
        result = users.join.inner(active_orders).select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"status": "active"})

    def it_generates_aliased_table_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, o.total
            FROM users
            INNER JOIN orders AS o ON (users.id = o.user_id)
        """
        users = vw.Source(name="users")
        o = vw.Source(name="orders").alias("o")
        result = (
            users.join.inner(o, on=[users.col("id") == o.col("user_id")])
            .select(users.col("name"), o.col("total"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_parameters():
    """Tests for parameterized values."""

    def it_generates_select_with_parameter_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON (users.id = orders.user_id)
                AND (users.status = :status)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        status_param = vw.param("status", "active")
        joined = users.join.inner(
            orders, on=[users.col("id") == orders.col("user_id"), users.col("status") == status_param]
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"status": "active"},
        )

    def it_generates_select_with_multiple_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders
                ON (users.id = :user_id)
                AND (orders.status = :status)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        user_id_param = vw.param("user_id", 123)
        status_param = vw.param("status", "pending")
        joined = users.join.inner(orders, on=[users.col("id") == user_id_param, orders.col("status") == status_param])
        result = joined.select(users.col("name"), orders.col("total")).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"user_id": 123, "status": "pending"},
        )

    def it_reuses_same_parameter_multiple_times(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON (users.age = :threshold)
                AND (orders.quantity = :threshold)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        threshold = vw.param("threshold", 100)
        joined = users.join.inner(orders, on=[users.col("age") == threshold, orders.col("quantity") == threshold])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"threshold": 100},
        )

    def it_supports_different_parameter_types(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON (users.name = :name)
                AND (users.age = :age)
                AND (orders.price = :price)
                AND (users.active = :active)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        name = vw.param("name", "Alice")
        age = vw.param("age", 25)
        price = vw.param("price", 19.99)
        active = vw.param("active", True)
        joined = users.join.inner(
            orders,
            on=[
                users.col("name") == name,
                users.col("age") == age,
                orders.col("price") == price,
                users.col("active") == active,
            ],
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"name": "Alice", "age": 25, "price": 19.99, "active": True},
        )


def describe_ctes():
    """Tests for Common Table Expressions (CTEs)."""

    def it_generates_basic_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (status = 'active'))
            SELECT * FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == vw.col("'active'")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_qualified_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT id, name FROM users)
            SELECT active_users.id, active_users.name FROM active_users
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")),
        )
        result = active_users.select(active_users.col("id"), active_users.col("name")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_in_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT id, name FROM users WHERE (active = true))
            SELECT orders.id, active_users.name
            FROM orders
            INNER JOIN active_users ON (orders.user_id = active_users.id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("id"), vw.col("name")).where(vw.col("active") == vw.col("true")),
        )
        orders = vw.Source(name="orders")
        result = (
            orders.join.inner(active_users, on=[orders.col("user_id") == active_users.col("id")])
            .select(orders.col("id"), active_users.col("name"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (status = :status))
            SELECT * FROM active_users
        """
        status = vw.param("status", "active")
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("status") == status),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"status": "active"})

    def it_generates_multiple_ctes(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH active_users AS (SELECT * FROM users WHERE (active = true)),
                 recent_orders AS (SELECT * FROM orders WHERE (created_at > '2024-01-01'))
            SELECT *
            FROM active_users
            INNER JOIN recent_orders ON (active_users.id = recent_orders.user_id)
        """
        active_users = vw.cte(
            "active_users",
            vw.Source(name="users").select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        recent_orders = vw.cte(
            "recent_orders",
            vw.Source(name="orders").select(vw.col("*")).where(vw.col("created_at") > vw.col("'2024-01-01'")),
        )
        result = (
            active_users.join.inner(recent_orders, on=[active_users.col("id") == recent_orders.col("user_id")])
            .select(vw.col("*"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cte_referencing_another_cte(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            WITH base_users AS (SELECT * FROM users),
                 active_users AS (SELECT * FROM base_users WHERE (active = true))
            SELECT * FROM active_users
        """
        base_users = vw.cte(
            "base_users",
            vw.Source(name="users").select(vw.col("*")),
        )
        active_users = vw.cte(
            "active_users",
            base_users.select(vw.col("*")).where(vw.col("active") == vw.col("true")),
        )
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_cast():
    """Tests for type casting."""

    def it_generates_cast_with_sqlalchemy_dialect(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, CAST(price AS DECIMAL(10,2)) FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.col("price").cast("DECIMAL(10,2)"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cast_with_postgres_dialect() -> None:
        expected_sql = """
            SELECT id, price::numeric FROM orders
        """
        config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.col("price").cast("numeric"))
            .render(config=config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cast_with_alias(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CAST(price AS DECIMAL(10,2)) AS formatted_price FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("price").cast("DECIMAL(10,2)").alias("formatted_price"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cast_with_parameter() -> None:
        expected_sql = """
            SELECT $value::VARCHAR FROM orders
        """
        config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
        result = (
            vw.Source(name="orders")
            .select(vw.param("value", 123).cast("VARCHAR"))
            .render(config=config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"value": 123})


def describe_expression_alias():
    """Tests for expression aliasing."""

    def it_generates_aliased_column_in_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, price AS unit_price FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.col("price").alias("unit_price"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_aliased_parameter_in_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, :tax_rate AS tax FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(vw.col("id"), vw.param("tax_rate", 0.08).alias("tax"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"tax_rate": 0.08})

    def it_generates_multiple_aliased_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT
                id AS order_id,
                price AS unit_price,
                quantity AS qty
            FROM orders
        """
        result = (
            vw.Source(name="orders")
            .select(
                vw.col("id").alias("order_id"),
                vw.col("price").alias("unit_price"),
                vw.col("quantity").alias("qty"),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})
