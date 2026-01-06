"""Tests for vw/build.py module."""

import pytest

import vw
from vw.build import CommonTableExpression, InnerJoin, Source, Statement
from vw.exceptions import CTENameCollisionError


def describe_source() -> None:
    """Tests for Source class."""

    def it_renders_source_name(render_context: vw.RenderContext) -> None:
        """Should render Source as its name."""
        source = Source(name="products")
        assert source.__vw_render__(render_context) == "products"

    def describe_col() -> None:
        """Tests for Source.col() method."""

        def it_returns_qualified_column(render_context: vw.RenderContext) -> None:
            """Should return Column with source name prefix."""
            source = Source(name="users")
            column = source.col("id")
            assert column.__vw_render__(render_context) == "users.id"

        def it_returns_column_equal_to_manually_constructed() -> None:
            """Should return Column equal to manually constructed qualified column."""
            source = Source(name="orders")
            assert source.col("user_id") == vw.Column(name="orders.user_id")

        def it_works_with_different_column_names(render_context: vw.RenderContext) -> None:
            """Should qualify any column name."""
            source = Source(name="orders")
            assert source.col("user_id").__vw_render__(render_context) == "orders.user_id"
            assert source.col("total").__vw_render__(render_context) == "orders.total"

    def describe_select() -> None:
        """Tests for Source.select() method."""

        def it_returns_statement() -> None:
            """Should return a Statement object."""
            source = Source(name="users")
            statement = source.select(vw.col("*"))
            assert isinstance(statement, Statement)

        def it_creates_statement_with_source_and_columns() -> None:
            """Should create Statement with correct source and columns."""
            source = Source(name="orders")
            col1 = vw.col("id")
            col2 = vw.col("name")
            statement = source.select(col1, col2)
            assert statement.source is source
            assert statement.columns == [col1, col2]


def describe_statement() -> None:
    """Tests for Statement class."""

    def describe_render() -> None:
        """Tests for Statement.render() method."""

        def it_renders_select_star(render_config: vw.RenderConfig) -> None:
            """Should render SELECT * FROM table."""
            source = Source(name="users")
            statement = Statement(source=source, columns=[vw.col("*")])
            assert statement.render(config=render_config) == vw.RenderResult(sql="SELECT * FROM users", params={})

        def it_renders_single_column(render_config: vw.RenderConfig) -> None:
            """Should render SELECT column FROM table."""
            source = Source(name="users")
            statement = Statement(source=source, columns=[vw.col("id")])
            assert statement.render(config=render_config) == vw.RenderResult(sql="SELECT id FROM users", params={})

        def it_renders_multiple_columns(render_config: vw.RenderConfig) -> None:
            """Should render SELECT col1, col2 FROM table."""
            source = Source(name="users")
            statement = Statement(source=source, columns=[vw.col("id"), vw.col("name"), vw.col("email")])
            assert statement.render(config=render_config) == vw.RenderResult(
                sql="SELECT id, name, email FROM users", params={}
            )


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

    def it_chains_multiple_joins(render_context: vw.RenderContext) -> None:
        """Should support chaining multiple joins."""
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


def describe_source_with_joins() -> None:
    """Tests for Source rendering with joins."""

    def it_renders_source_with_single_join(render_context: vw.RenderContext) -> None:
        """Should render source with INNER JOIN."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        assert joined.__vw_render__(render_context) == "users INNER JOIN orders ON (users.id = orders.user_id)"

    def it_renders_select_statement_with_join(render_config: vw.RenderConfig) -> None:
        """Should render SELECT statement with join."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(vw.col("*"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users INNER JOIN orders ON (users.id = orders.user_id)", params={}
        )


def describe_where() -> None:
    """Tests for WHERE clause functionality."""

    def it_returns_statement() -> None:
        """Should return a Statement object."""
        source = Source(name="users")
        statement = source.select(vw.col("*")).where(vw.col("age") >= vw.col("18"))
        assert isinstance(statement, Statement)

    def it_renders_where_with_single_condition(render_config: vw.RenderConfig) -> None:
        """Should render SELECT with WHERE clause."""
        source = Source(name="users")
        statement = source.select(vw.col("*")).where(vw.col("age") >= vw.col("18"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users WHERE (age >= 18)", params={}
        )

    def it_renders_where_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        """Should render WHERE with multiple conditions combined with AND."""
        source = Source(name="users")
        statement = source.select(vw.col("*")).where(
            vw.col("age") >= vw.col("18"), vw.col("status") == vw.col("'active'")
        )
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users WHERE (age >= 18) AND (status = 'active')", params={}
        )

    def it_renders_where_with_parameters(render_config: vw.RenderConfig) -> None:
        """Should render WHERE clause with parameterized values."""
        source = Source(name="users")
        min_age = vw.param("min_age", 18)
        status = vw.param("status", "active")
        statement = source.select(vw.col("*")).where(vw.col("age") >= min_age, vw.col("status") == status)
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users WHERE (age >= :min_age) AND (status = :status)",
            params={"min_age": 18, "status": "active"},
        )

    def it_chains_multiple_where_calls(render_config: vw.RenderConfig) -> None:
        """Should support chaining multiple where() calls."""
        source = Source(name="users")
        statement = (
            source.select(vw.col("*"))
            .where(vw.col("age") >= vw.col("18"))
            .where(vw.col("status") == vw.col("'active'"))
        )
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users WHERE (age >= 18) AND (status = 'active')", params={}
        )

    def it_renders_where_with_join(render_config: vw.RenderConfig) -> None:
        """Should render WHERE clause with JOIN."""
        users = Source(name="users")
        orders = Source(name="orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(vw.col("*")).where(orders.col("total") > vw.col("100"))
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM users INNER JOIN orders ON (users.id = orders.user_id) WHERE (orders.total > 100)",
            params={},
        )

    def it_renders_where_with_all_comparison_operators(render_config: vw.RenderConfig) -> None:
        """Should render WHERE clause with all comparison operators."""
        source = Source(name="products")
        statement = source.select(vw.col("*")).where(
            vw.col("price") > vw.col("10"),
            vw.col("stock") >= vw.col("5"),
            vw.col("discount") < vw.col("50"),
            vw.col("rating") <= vw.col("4.5"),
            vw.col("active") == vw.col("true"),
            vw.col("deleted") != vw.col("true"),
        )
        assert statement.render(config=render_config) == vw.RenderResult(
            sql="SELECT * FROM products WHERE (price > 10) AND (stock >= 5) AND (discount < 50) AND (rating <= 4.5) AND (active = true) AND (deleted <> true)",
            params={},
        )


def describe_common_table_expression() -> None:
    """Tests for CommonTableExpression class."""

    def it_renders_cte_name(render_context: vw.RenderContext) -> None:
        """Should render CTE as its name and register in context."""
        query = Source(name="users").select(vw.col("*"))
        cte = CommonTableExpression(name="active_users", query=query)
        assert cte.__vw_render__(render_context) == "active_users"
        assert render_context.ctes == [(cte, "(SELECT * FROM users)")]

    def it_renders_cte_with_alias(render_context: vw.RenderContext) -> None:
        """Should render CTE with alias."""
        query = Source(name="users").select(vw.col("*"))
        cte = CommonTableExpression(name="active_users", query=query).alias("au")
        assert cte.__vw_render__(render_context) == "active_users AS au"

    def describe_col() -> None:
        """Tests for CommonTableExpression.col() method."""

        def it_returns_qualified_column(render_context: vw.RenderContext) -> None:
            """Should return Column with CTE name prefix."""
            query = Source(name="users").select(vw.col("*"))
            cte = CommonTableExpression(name="active_users", query=query)
            column = cte.col("id")
            assert column.__vw_render__(render_context) == "active_users.id"

        def it_uses_alias_when_set(render_context: vw.RenderContext) -> None:
            """Should use alias as prefix when set."""
            query = Source(name="users").select(vw.col("*"))
            cte = CommonTableExpression(name="active_users", query=query).alias("au")
            column = cte.col("id")
            assert column.__vw_render__(render_context) == "au.id"

    def describe_select() -> None:
        """Tests for CommonTableExpression.select() method."""

        def it_returns_statement() -> None:
            """Should return a Statement object."""
            query = Source(name="users").select(vw.col("*"))
            cte = CommonTableExpression(name="active_users", query=query)
            statement = cte.select(vw.col("*"))
            assert isinstance(statement, Statement)

        def it_renders_with_clause(render_config: vw.RenderConfig) -> None:
            """Should render WITH clause when CTE is used."""
            query = Source(name="users").select(vw.col("*"))
            cte = CommonTableExpression(name="active_users", query=query)
            result = cte.select(vw.col("*")).render(config=render_config)
            assert result == vw.RenderResult(
                sql="WITH active_users AS (SELECT * FROM users) SELECT * FROM active_users",
                params={},
            )

    def describe_join() -> None:
        """Tests for CommonTableExpression in joins."""

        def it_renders_cte_in_join(render_config: vw.RenderConfig) -> None:
            """Should render CTE in JOIN clause."""
            query = Source(name="users").select(vw.col("id"), vw.col("name"))
            active_users = CommonTableExpression(name="active_users", query=query)
            orders = Source(name="orders")
            result = (
                orders.join.inner(active_users, on=[orders.col("user_id") == active_users.col("id")])
                .select(vw.col("*"))
                .render(config=render_config)
            )
            assert result == vw.RenderResult(
                sql="WITH active_users AS (SELECT id, name FROM users) SELECT * FROM orders INNER JOIN active_users ON (orders.user_id = active_users.id)",
                params={},
            )


def describe_cte_function() -> None:
    """Tests for cte() convenience function."""

    def it_creates_common_table_expression() -> None:
        """Should create a CommonTableExpression instance."""
        query = Source(name="users").select(vw.col("*"))
        result = vw.cte("active_users", query)
        assert result == CommonTableExpression(name="active_users", query=query)

    def it_renders_with_clause(render_config: vw.RenderConfig) -> None:
        """Should render WITH clause when used."""
        active_users = vw.cte("active_users", Source(name="users").select(vw.col("*")))
        result = active_users.select(vw.col("*")).render(config=render_config)
        assert result == vw.RenderResult(
            sql="WITH active_users AS (SELECT * FROM users) SELECT * FROM active_users",
            params={},
        )

    def it_raises_error_on_cte_name_collision(render_config: vw.RenderConfig) -> None:
        """Should raise CTENameCollisionError when two different CTEs have the same name."""
        cte1 = vw.cte("users", Source(name="active_users").select(vw.col("*")))
        cte2 = vw.cte("users", Source(name="inactive_users").select(vw.col("*")))
        with pytest.raises(CTENameCollisionError):
            cte1.join.inner(cte2).select(vw.col("*")).render(config=render_config)
