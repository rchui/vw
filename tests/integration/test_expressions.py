"""Integration tests for expressions: cast, alias, parameters, null handling."""

import vw
from tests.utils import sql


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
        result = vw.Source(name="orders").select(vw.col("id"), vw.col("price").cast("numeric")).render(config=config)
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
        result = vw.Source(name="orders").select(vw.param("value", 123).cast("VARCHAR")).render(config=config)
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


def describe_parameters():
    """Tests for parameterized values."""

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


def describe_null_handling():
    """Tests for NULL handling in queries."""

    def it_generates_where_with_is_null(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (deleted_at IS NULL)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("deleted_at").is_null())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_where_with_is_not_null(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (email IS NOT NULL)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("email").is_not_null())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_combines_null_check_with_other_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (deleted_at IS NULL) AND (status = 'active')
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("deleted_at").is_null(), vw.col("status") == vw.col("'active'"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_null_check_with_left_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.id, users.name
            FROM users
            LEFT JOIN orders ON (users.id = orders.user_id)
            WHERE (orders.id IS NULL)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("id"), users.col("name"))
            .where(orders.col("id").is_null())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_null_check_with_logical_or(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE ((email IS NOT NULL) OR (phone IS NOT NULL))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("email").is_not_null() | vw.col("phone").is_not_null())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_null_check_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (deleted_at IS NULL) AND (status = :status)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("deleted_at").is_null(), vw.col("status") == vw.param("status", "active"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"status": "active"})


def describe_complex_expressions():
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


def describe_star_extensions():
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


def describe_in_subqueries():
    """Tests for IN and NOT IN expressions."""

    def it_generates_in_with_literal_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (status IN ('active', 'pending'))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("status").is_in(vw.col("'active'"), vw.col("'pending'")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_not_in_with_literal_values(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (status NOT IN ('deleted', 'archived'))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("status").is_not_in(vw.col("'deleted'"), vw.col("'archived'")))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_in_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users WHERE (id IN (:id1, :id2, :id3))
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("id").is_in(vw.param("id1", 1), vw.param("id2", 2), vw.param("id3", 3)))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"id1": 1, "id2": 2, "id3": 3})

    def it_generates_in_with_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
            WHERE (id IN ((SELECT user_id FROM orders WHERE (total > :min_total))))
        """
        orders_subquery = (
            vw.Source(name="orders").select(vw.col("user_id")).where(vw.col("total") > vw.param("min_total", 100))
        )
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("id").is_in(orders_subquery))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"min_total": 100})

    def it_generates_not_in_with_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM products
            WHERE (id NOT IN ((SELECT product_id FROM discontinued)))
        """
        discontinued_subquery = vw.Source(name="discontinued").select(vw.col("product_id"))
        result = (
            vw.Source(name="products")
            .select(vw.col("*"))
            .where(vw.col("id").is_not_in(discontinued_subquery))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_combines_in_with_other_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
            WHERE (status IN ('active', 'pending')) AND (age >= :min_age)
        """
        result = (
            vw.Source(name="users")
            .select(vw.col("*"))
            .where(
                vw.col("status").is_in(vw.col("'active'"), vw.col("'pending'")),
                vw.col("age") >= vw.param("min_age", 18),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"min_age": 18})


def describe_exists_subqueries():
    """Tests for EXISTS expressions."""

    def it_generates_exists_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
            WHERE (EXISTS (SELECT 1 FROM orders WHERE (orders.user_id = users.id)))
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        exists_subquery = orders.select(vw.col("1")).where(orders.col("user_id") == users.col("id"))
        result = users.select(vw.col("*")).where(vw.exists(exists_subquery)).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_not_exists_subquery(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
            WHERE (NOT (EXISTS (SELECT 1 FROM orders WHERE (orders.user_id = users.id))))
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        exists_subquery = orders.select(vw.col("1")).where(orders.col("user_id") == users.col("id"))
        result = users.select(vw.col("*")).where(~vw.exists(exists_subquery)).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_exists_with_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM customers
            WHERE (EXISTS (SELECT 1 FROM orders WHERE (orders.customer_id = customers.id) AND (orders.total > :min_total)))
        """
        customers = vw.Source(name="customers")
        orders = vw.Source(name="orders")
        exists_subquery = orders.select(vw.col("1")).where(
            orders.col("customer_id") == customers.col("id"),
            orders.col("total") > vw.param("min_total", 1000),
        )
        result = customers.select(vw.col("*")).where(vw.exists(exists_subquery)).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={"min_total": 1000})

    def it_combines_exists_with_other_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * FROM users
            WHERE (status = 'active')
            AND (EXISTS (SELECT 1 FROM orders WHERE (orders.user_id = users.id)))
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        exists_subquery = orders.select(vw.col("1")).where(orders.col("user_id") == users.col("id"))
        result = (
            users.select(vw.col("*"))
            .where(
                vw.col("status") == vw.col("'active'"),
                vw.exists(exists_subquery),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_method_chaining():
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
