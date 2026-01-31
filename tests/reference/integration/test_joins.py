"""Integration tests for JOIN operations and subqueries."""

import vw.reference as vw
from tests.utils import sql


def describe_inner_joins():
    """Tests for INNER JOIN operations."""

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


def describe_left_joins():
    """Tests for LEFT JOIN operations."""

    def it_generates_left_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            LEFT JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_left_join_with_where(render_config: vw.RenderConfig) -> None:
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


def describe_right_joins():
    """Tests for RIGHT JOIN operations."""

    def it_generates_right_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            RIGHT JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.right(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_right_join_with_where(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT orders.id, orders.total
            FROM users
            RIGHT JOIN orders ON (users.id = orders.user_id)
            WHERE (users.id IS NULL)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.right(orders, on=[users.col("id") == orders.col("user_id")])
            .select(orders.col("id"), orders.col("total"))
            .where(users.col("id").is_null())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_full_outer_joins():
    """Tests for FULL OUTER JOIN operations."""

    def it_generates_full_outer_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            FULL OUTER JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_full_outer_join_with_coalesce(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT
                COALESCE(users.id, orders.user_id) AS id,
                users.name,
                orders.total
            FROM users
            FULL OUTER JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.full_outer(orders, on=[users.col("id") == orders.col("user_id")])
            .select(
                vw.col("COALESCE(users.id, orders.user_id)").alias("id"),
                users.col("name"),
                orders.col("total"),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_cross_joins():
    """Tests for CROSS JOIN operations."""

    def it_generates_cross_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM colors
            CROSS JOIN sizes
        """
        colors = vw.Source(name="colors")
        sizes = vw.Source(name="sizes")
        joined = colors.join.cross(sizes)
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_cross_join_with_selected_columns(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT colors.name, sizes.label
            FROM colors
            CROSS JOIN sizes
        """
        colors = vw.Source(name="colors")
        sizes = vw.Source(name="sizes")
        result = colors.join.cross(sizes).select(colors.col("name"), sizes.col("label")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_multiple_cross_joins(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM colors
            CROSS JOIN sizes
            CROSS JOIN materials
        """
        colors = vw.Source(name="colors")
        sizes = vw.Source(name="sizes")
        materials = vw.Source(name="materials")
        result = colors.join.cross(sizes).join.cross(materials).select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_semi_joins():
    """Tests for SEMI JOIN operations."""

    def it_generates_semi_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            SEMI JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.semi(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_semi_join_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            SEMI JOIN orders
                ON (users.id = orders.user_id)
                AND (orders.status = 'active')
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.semi(
            orders,
            on=[users.col("id") == orders.col("user_id"), orders.col("status") == vw.col("'active'")],
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_anti_joins():
    """Tests for ANTI JOIN operations."""

    def it_generates_anti_join(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            ANTI JOIN orders ON (users.id = orders.user_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.anti(orders, on=[users.col("id") == orders.col("user_id")])
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})

    def it_generates_anti_join_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            ANTI JOIN orders
                ON (users.id = orders.user_id)
                AND (orders.status = 'cancelled')
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        joined = users.join.anti(
            orders,
            on=[users.col("id") == orders.col("user_id"), orders.col("status") == vw.col("'cancelled'")],
        )
        result = joined.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_mixed_joins():
    """Tests for mixed join types."""

    def it_chains_inner_and_left_joins(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total, refunds.amount
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
            LEFT JOIN refunds ON (orders.id = refunds.order_id)
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        refunds = vw.Source(name="refunds")
        result = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .join.left(refunds, on=[orders.col("id") == refunds.col("order_id")])
            .select(users.col("name"), orders.col("total"), refunds.col("amount"))
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
            FROM (
                SELECT *
                FROM (
                    SELECT id
                    FROM users
                ) AS inner_q
            ) AS outer_q
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
            INNER JOIN (SELECT user_id FROM orders WHERE (status = $status)) AS active_orders
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


def describe_joins_with_clauses():
    """Tests for joins combined with other clauses."""

    def it_generates_join_with_where(render_config: vw.RenderConfig) -> None:
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

    def it_generates_join_with_group_by(render_config: vw.RenderConfig) -> None:
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

    def it_generates_join_with_order_by(render_config: vw.RenderConfig) -> None:
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

    def it_generates_join_with_limit(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders ON (users.id = orders.user_id)
            ORDER BY orders.total DESC
            LIMIT 5
        """
        users = vw.Source(name="users")
        orders = vw.Source(name="orders")
        result = (
            users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
            .select(users.col("name"), orders.col("total"))
            .order_by(orders.col("total").desc())
            .limit(5)
            .render(config=render_config)
        )
        assert result == vw.RenderResult(sql=sql(expected_sql), params={})


def describe_parameters_in_joins():
    """Tests for parameterized values in joins."""

    def it_generates_join_with_parameter_in_condition(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT *
            FROM users
            INNER JOIN orders
                ON (users.id = orders.user_id)
                AND (users.status = $status)
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

    def it_generates_join_with_multiple_parameters(render_config: vw.RenderConfig) -> None:
        expected_sql = """
            SELECT users.name, orders.total
            FROM users
            INNER JOIN orders
                ON (users.id = $user_id)
                AND (orders.status = $status)
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
                ON (users.age = $threshold)
                AND (orders.quantity = $threshold)
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
