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
