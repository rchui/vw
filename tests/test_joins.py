"""Tests for vw/joins.py module."""

import vw
from vw.build import Source
from vw.joins import InnerJoin, LeftJoin


def describe_inner_join() -> None:
    """Tests for InnerJoin class."""

    def it_renders_inner_join_without_condition(render_context: vw.RenderContext) -> None:
        """Should render INNER JOIN without ON clause."""
        orders = Source(name="orders")
        join = InnerJoin(right=orders)
        assert join.__vw_render__(render_context) == "INNER JOIN orders"

    def it_renders_inner_join_with_single_condition(render_context: vw.RenderContext) -> None:
        """Should render INNER JOIN with ON clause."""
        users = Source(name="users")
        orders = Source(name="orders")
        condition = users.col("id") == orders.col("user_id")
        join = InnerJoin(right=orders, on=[condition])
        assert join.__vw_render__(render_context) == "INNER JOIN orders ON (users.id = orders.user_id)"

    def it_renders_inner_join_with_multiple_conditions(render_context: vw.RenderContext) -> None:
        """Should render INNER JOIN with multiple conditions combined with AND."""
        users = Source(name="users")
        orders = Source(name="orders")
        condition1 = users.col("id") == orders.col("user_id")
        condition2 = users.col("status") == vw.col("'active'")
        join = InnerJoin(right=orders, on=[condition1, condition2])
        assert (
            join.__vw_render__(render_context)
            == "INNER JOIN orders ON (users.id = orders.user_id) AND (users.status = 'active')"
        )


def describe_left_join() -> None:
    """Tests for LeftJoin class."""

    def it_renders_left_join_without_condition(render_context: vw.RenderContext) -> None:
        """Should render LEFT JOIN without ON clause."""
        orders = Source(name="orders")
        join = LeftJoin(right=orders)
        assert join.__vw_render__(render_context) == "LEFT JOIN orders"

    def it_renders_left_join_with_single_condition(render_context: vw.RenderContext) -> None:
        """Should render LEFT JOIN with ON clause."""
        users = Source(name="users")
        orders = Source(name="orders")
        condition = users.col("id") == orders.col("user_id")
        join = LeftJoin(right=orders, on=[condition])
        assert join.__vw_render__(render_context) == "LEFT JOIN orders ON (users.id = orders.user_id)"

    def it_renders_left_join_with_multiple_conditions(render_context: vw.RenderContext) -> None:
        """Should render LEFT JOIN with multiple conditions combined with AND."""
        users = Source(name="users")
        orders = Source(name="orders")
        condition1 = users.col("id") == orders.col("user_id")
        condition2 = orders.col("status") == vw.col("'pending'")
        join = LeftJoin(right=orders, on=[condition1, condition2])
        assert (
            join.__vw_render__(render_context)
            == "LEFT JOIN orders ON (users.id = orders.user_id) AND (orders.status = 'pending')"
        )


def describe_join_accessor() -> None:
    """Tests for JoinAccessor class."""

    def it_creates_source_with_inner_join() -> None:
        """Should create a new Source with inner join."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        assert isinstance(joined, Source)
        assert len(joined._joins) == 1
        assert isinstance(joined._joins[0], InnerJoin)

    def it_creates_source_with_left_join() -> None:
        """Should create a new Source with left join."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        assert isinstance(joined, Source)
        assert len(joined._joins) == 1
        assert isinstance(joined._joins[0], LeftJoin)

    def it_chains_multiple_inner_joins(render_context: vw.RenderContext) -> None:
        """Should support chaining multiple inner joins."""
        users = Source(name="users")
        orders = Source(name="orders")
        products = Source(name="products")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        joined = joined.join.inner(products, on=[orders.col("product_id") == products.col("id")])
        assert len(joined._joins) == 2
        assert (
            joined.__vw_render__(render_context)
            == "users INNER JOIN orders ON (users.id = orders.user_id) INNER JOIN products ON (orders.product_id = products.id)"
        )

    def it_chains_multiple_left_joins(render_context: vw.RenderContext) -> None:
        """Should support chaining multiple left joins."""
        users = Source(name="users")
        orders = Source(name="orders")
        products = Source(name="products")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        joined = joined.join.left(products, on=[orders.col("product_id") == products.col("id")])
        assert len(joined._joins) == 2
        assert (
            joined.__vw_render__(render_context)
            == "users LEFT JOIN orders ON (users.id = orders.user_id) LEFT JOIN products ON (orders.product_id = products.id)"
        )

    def it_chains_mixed_join_types(render_context: vw.RenderContext) -> None:
        """Should support chaining inner and left joins together."""
        users = Source(name="users")
        orders = Source(name="orders")
        refunds = Source(name="refunds")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        joined = joined.join.left(refunds, on=[orders.col("id") == refunds.col("order_id")])
        assert len(joined._joins) == 2
        assert isinstance(joined._joins[0], InnerJoin)
        assert isinstance(joined._joins[1], LeftJoin)
        assert (
            joined.__vw_render__(render_context)
            == "users INNER JOIN orders ON (users.id = orders.user_id) LEFT JOIN refunds ON (orders.id = refunds.order_id)"
        )


def describe_source_with_joins() -> None:
    """Tests for Source rendering with joins."""

    def it_renders_source_with_single_inner_join(render_context: vw.RenderContext) -> None:
        """Should render source with INNER JOIN."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        assert joined.__vw_render__(render_context) == "users INNER JOIN orders ON (users.id = orders.user_id)"

    def it_renders_source_with_single_left_join(render_context: vw.RenderContext) -> None:
        """Should render source with LEFT JOIN."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        assert joined.__vw_render__(render_context) == "users LEFT JOIN orders ON (users.id = orders.user_id)"

    def it_renders_select_statement_with_inner_join(render_config: vw.RenderConfig) -> None:
        """Should render SELECT statement with inner join."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(vw.col("*"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users INNER JOIN orders ON (users.id = orders.user_id)", params={}
        )

    def it_renders_select_statement_with_left_join(render_config: vw.RenderConfig) -> None:
        """Should render SELECT statement with left join."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(vw.col("*"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users LEFT JOIN orders ON (users.id = orders.user_id)", params={}
        )

    def it_renders_left_join_with_where_clause(render_config: vw.RenderConfig) -> None:
        """Should render LEFT JOIN with WHERE clause filtering for NULLs."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.left(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(users.col("id"), users.col("name")).where(orders.col("id") == vw.col("NULL"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT users.id, users.name FROM users LEFT JOIN orders ON (users.id = orders.user_id) WHERE (orders.id = NULL)",
            params={},
        )
