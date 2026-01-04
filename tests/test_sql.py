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
        result = vw.Source("users").select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_select_single_column(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id FROM products
        """
        result = vw.Source("products").select(vw.col("id")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_select_multiple_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, customer_id, total FROM orders
        """
        result = (
            vw.Source("orders")
            .select(vw.col("id"), vw.col("customer_id"), vw.col("total"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_star_extensions() -> None:
    """Tests for star expression extensions."""

    def it_generates_star_replace(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * REPLACE (name AS full_name) FROM users
        """
        result = vw.Source("users").select(vw.col("* REPLACE (name AS full_name)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_star_exclude(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * EXCLUDE (password) FROM users
        """
        result = vw.Source("users").select(vw.col("* EXCLUDE (password)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_star_exclude_multiple(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT * EXCLUDE (password, ssn) FROM users
        """
        result = vw.Source("users").select(vw.col("* EXCLUDE (password, ssn)")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_method_chaining() -> None:
    """Tests for method chaining patterns."""

    def it_chains_source_select_render() -> None:
        expected_sql = """
            SELECT id, name FROM employees
        """
        result = vw.Source("employees").select(vw.col("id"), vw.col("name")).render(config=vw.RenderConfig())
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_allows_breaking_chain_into_steps() -> None:
        expected_sql = """
            SELECT * FROM products
        """
        source = vw.Source("products")
        statement = source.select(vw.col("*"))
        result = statement.render(config=vw.RenderConfig())
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_complex_expressions() -> None:
    """Tests for complex SQL expressions via escape hatch."""

    def it_generates_cast_expression(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT CAST(price AS DECIMAL(10,2)) FROM sales
        """
        result = vw.Source("sales").select(vw.col("CAST(price AS DECIMAL(10,2))")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_mixes_simple_and_complex_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT id, ROUND(total, 2) AS rounded_total, status FROM orders
        """
        result = (
            vw.Source("orders")
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
            INNER JOIN orders ON users.id = orders.user_id
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_inner_join_with_selected_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders ON users.id = orders.user_id
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(users.col("name"), orders.col("total")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_inner_join_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON users.id = orders.user_id
                AND users.status = 'active'
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        joined = users.join.inner(
            orders, on=[users.col("id") == orders.col("user_id"), users.col("status") == vw.col("'active'")]
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_multiple_joins(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.quantity, products.price
            FROM users
            INNER JOIN orders ON users.id = orders.user_id
            INNER JOIN products ON orders.product_id = products.id
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        products = vw.Source("products")
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
        users = vw.Source("users")
        settings = vw.Source("settings")
        joined = users.join.inner(settings)
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_chains_join_and_select(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders ON users.id = orders.user_id
        """
        result = (
            vw.Source("users")
            .join.inner(vw.Source("orders"), on=[vw.Source("users").col("id") == vw.Source("orders").col("user_id")])
            .select(vw.col("*"))
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
                ON users.id = orders.user_id
                AND users.status = :status
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        status_param = vw.param("status", "active")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id"), users.col("status") == status_param])
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
                ON users.id = :user_id
                AND orders.status = :status
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        user_id_param = vw.param("user_id", 123)
        status_param = vw.param("status", "pending")
        joined = users.join.inner(
            orders, on=[users.col("id") == user_id_param, orders.col("status") == status_param]
        )
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
                ON users.age = :threshold
                AND orders.quantity = :threshold
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
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
                ON users.name = :name
                AND users.age = :age
                AND orders.price = :price
                AND users.active = :active
        """
        users = vw.Source("users")
        orders = vw.Source("orders")
        name = vw.param("name", "Alice")
        age = vw.param("age", 25)
        price = vw.param("price", 19.99)
        active = vw.param("active", True)
        joined = users.join.inner(
            orders,
            on=[users.col("name") == name, users.col("age") == age, orders.col("price") == price, users.col("active") == active],
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(
            sql=sql(expected_sql),
            params={"name": "Alice", "age": 25, "price": 19.99, "active": True},
        )
