"""Tests for vw/build.py module."""

import pytest

import vw
from vw import dtypes
from vw.build import CommonTableExpression, Source, Statement
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


def describe_cast() -> None:
    """Tests for expression casting."""

    def it_casts_column_with_function_style(render_context: vw.RenderContext) -> None:
        """Should render CAST(column AS type) for SQLAlchemy dialect."""
        expr = vw.col("price").cast(dtypes.decimal(10, 2))
        assert expr.__vw_render__(render_context) == "CAST(price AS DECIMAL(10,2))"

    def it_casts_column_with_operator_style() -> None:
        """Should render column::type for PostgreSQL dialect."""
        config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
        context = vw.RenderContext(config=config)
        expr = vw.col("price").cast(dtypes.numeric())
        assert expr.__vw_render__(context) == "price::NUMERIC"

    def it_casts_parameter(render_context: vw.RenderContext) -> None:
        """Should render CAST(parameter AS type)."""
        expr = vw.param("value", 123).cast(dtypes.varchar())
        assert expr.__vw_render__(render_context) == "CAST(:value AS VARCHAR)"
        assert render_context.params == {"value": 123}

    def it_casts_with_sqlserver_dialect() -> None:
        """Should render CAST() for SQL Server dialect."""
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        context = vw.RenderContext(config=config)
        expr = vw.col("price").cast(dtypes.decimal(10, 2))
        assert expr.__vw_render__(context) == "CAST(price AS DECIMAL(10,2))"

    def it_chains_cast_and_alias(render_context: vw.RenderContext) -> None:
        """Should allow chaining cast and alias."""
        expr = vw.col("price").cast(dtypes.decimal(10, 2)).alias("formatted_price")
        assert expr.__vw_render__(render_context) == "CAST(price AS DECIMAL(10,2)) AS formatted_price"

    def it_renders_in_select(render_config: vw.RenderConfig) -> None:
        """Should render cast in SELECT."""
        result = (
            Source(name="orders")
            .select(
                vw.col("id"),
                vw.col("price").cast(dtypes.decimal(10, 2)),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT id, CAST(price AS DECIMAL(10,2)) FROM orders",
            params={},
        )

    def it_can_be_raw() -> None:
        """Should allow raw dtypes."""
        vw.col("price").cast(dtypes.dtype("INTEGER"))


def describe_alias() -> None:
    """Tests for expression aliasing."""

    def it_aliases_column(render_context: vw.RenderContext) -> None:
        """Should render column AS alias."""
        expr = vw.col("price").alias("unit_price")
        assert expr.__vw_render__(render_context) == "price AS unit_price"

    def it_aliases_parameter(render_context: vw.RenderContext) -> None:
        """Should render parameter AS alias."""
        expr = vw.param("tax_rate", 0.08).alias("tax")
        assert expr.__vw_render__(render_context) == ":tax_rate AS tax"
        assert render_context.params == {"tax_rate": 0.08}

    def it_aliases_comparison_expression(render_context: vw.RenderContext) -> None:
        """Should render comparison expression AS alias."""
        expr = (vw.col("age") >= vw.col("18")).alias("is_adult")
        assert expr.__vw_render__(render_context) == "age >= 18 AS is_adult"

    def it_aliases_logical_expression(render_context: vw.RenderContext) -> None:
        """Should render logical expression AS alias."""
        expr = (vw.col("a") & vw.col("b")).alias("both")
        assert expr.__vw_render__(render_context) == "(a) AND (b) AS both"

    def it_renders_in_select(render_config: vw.RenderConfig) -> None:
        """Should render aliased expression in SELECT."""
        result = (
            Source(name="orders")
            .select(
                vw.col("id"),
                vw.col("price").alias("unit_price"),
            )
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT id, price AS unit_price FROM orders",
            params={},
        )


def describe_group_by() -> None:
    """Tests for GROUP BY clause."""

    def it_returns_statement() -> None:
        """Should return a Statement object."""
        source = Source(name="orders")
        statement = source.select(vw.col("customer_id")).group_by(vw.col("customer_id"))
        assert isinstance(statement, Statement)

    def it_renders_group_by_single_column(render_config: vw.RenderConfig) -> None:
        """Should render GROUP BY with single column."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id",
            params={},
        )

    def it_renders_group_by_multiple_columns(render_config: vw.RenderConfig) -> None:
        """Should render GROUP BY with multiple columns."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("status"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"), vw.col("status"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, status, COUNT(*) FROM orders GROUP BY customer_id, status",
            params={},
        )

    def it_chains_multiple_group_by_calls(render_config: vw.RenderConfig) -> None:
        """Should support chaining multiple group_by() calls."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("status"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .group_by(vw.col("status"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, status, COUNT(*) FROM orders GROUP BY customer_id, status",
            params={},
        )

    def it_renders_with_where_clause(render_config: vw.RenderConfig) -> None:
        """Should render GROUP BY after WHERE clause."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.col("customer_id"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, SUM(total) FROM orders WHERE (status = 'completed') GROUP BY customer_id",
            params={},
        )


def describe_order_by() -> None:
    """Tests for ORDER BY clause."""

    def it_returns_statement() -> None:
        """Should return a Statement object."""
        source = Source(name="users")
        statement = source.select(vw.col("*")).order_by(vw.col("name").asc())
        assert isinstance(statement, Statement)

    def it_renders_order_by_single_column(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY with single column."""
        result = Source(name="users").select(vw.col("*")).order_by(vw.col("name").asc()).render(config=render_config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY name ASC",
            params={},
        )

    def it_renders_order_by_without_direction(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY without explicit direction."""
        result = Source(name="users").select(vw.col("*")).order_by(vw.col("name")).render(config=render_config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY name",
            params={},
        )

    def it_renders_order_by_desc(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY with DESC."""
        result = (
            Source(name="users").select(vw.col("*")).order_by(vw.col("created_at").desc()).render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY created_at DESC",
            params={},
        )

    def it_renders_order_by_multiple_columns(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY with multiple columns."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("last_name").asc(), vw.col("first_name").asc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY last_name ASC, first_name ASC",
            params={},
        )

    def it_renders_order_by_mixed_directions(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY with mixed ASC and DESC."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("status").asc(), vw.col("created_at").desc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY status ASC, created_at DESC",
            params={},
        )

    def it_chains_multiple_order_by_calls(render_config: vw.RenderConfig) -> None:
        """Should support chaining multiple order_by() calls."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("last_name").asc())
            .order_by(vw.col("first_name").asc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY last_name ASC, first_name ASC",
            params={},
        )

    def it_renders_with_where_clause(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY after WHERE clause."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .where(vw.col("active") == vw.col("true"))
            .order_by(vw.col("name").asc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users WHERE (active = true) ORDER BY name ASC",
            params={},
        )

    def it_renders_with_group_by_and_having(render_config: vw.RenderConfig) -> None:
        """Should render ORDER BY after GROUP BY and HAVING."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("SUM(total)") > vw.col("100"))
            .order_by(vw.col("SUM(total)").desc())
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id HAVING (SUM(total) > 100) ORDER BY SUM(total) DESC",
            params={},
        )


def describe_limit() -> None:
    """Tests for LIMIT clause."""

    def it_returns_statement() -> None:
        """Should return a Statement object."""
        source = Source(name="users")
        statement = source.select(vw.col("*")).limit(10)
        assert isinstance(statement, Statement)

    def it_renders_limit_only(render_config: vw.RenderConfig) -> None:
        """Should render LIMIT without OFFSET."""
        result = Source(name="users").select(vw.col("*")).limit(10).render(config=render_config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users LIMIT 10",
            params={},
        )

    def it_renders_limit_with_offset(render_config: vw.RenderConfig) -> None:
        """Should render LIMIT with OFFSET."""
        result = Source(name="users").select(vw.col("*")).limit(10, offset=20).render(config=render_config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users LIMIT 10 OFFSET 20",
            params={},
        )

    def it_renders_with_order_by(render_config: vw.RenderConfig) -> None:
        """Should render LIMIT after ORDER BY."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("id").asc())
            .limit(10, offset=20)
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20",
            params={},
        )

    def it_renders_sqlserver_dialect() -> None:
        """Should render OFFSET/FETCH for SQL Server dialect."""
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .order_by(vw.col("id").asc())
            .limit(10, offset=20)
            .render(config=config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY id ASC OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY",
            params={},
        )

    def it_renders_sqlserver_without_offset() -> None:
        """Should render OFFSET 0 for SQL Server when no offset specified."""
        config = vw.RenderConfig(dialect=vw.Dialect.SQLSERVER)
        result = Source(name="users").select(vw.col("*")).order_by(vw.col("id").asc()).limit(10).render(config=config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users ORDER BY id ASC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY",
            params={},
        )

    def it_renders_postgres_dialect() -> None:
        """Should render LIMIT/OFFSET for PostgreSQL dialect."""
        config = vw.RenderConfig(dialect=vw.Dialect.POSTGRES)
        result = Source(name="users").select(vw.col("*")).limit(10, offset=5).render(config=config)
        assert result == vw.RenderResult(
            sql="SELECT * FROM users LIMIT 10 OFFSET 5",
            params={},
        )

    def it_chains_with_other_clauses(render_config: vw.RenderConfig) -> None:
        """Should chain with WHERE, GROUP BY, HAVING, and ORDER BY."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("SUM(total)"))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("SUM(total)") > vw.col("100"))
            .order_by(vw.col("SUM(total)").desc())
            .limit(10)
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, SUM(total) FROM orders WHERE (status = 'completed') GROUP BY customer_id HAVING (SUM(total) > 100) ORDER BY SUM(total) DESC LIMIT 10",
            params={},
        )

    def it_preserves_limit_through_other_methods(render_config: vw.RenderConfig) -> None:
        """Should preserve limit when chaining other methods after it."""
        result = (
            Source(name="users")
            .select(vw.col("*"))
            .limit(10, offset=5)
            .where(vw.col("active") == vw.col("true"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT * FROM users WHERE (active = true) LIMIT 10 OFFSET 5",
            params={},
        )


def describe_having() -> None:
    """Tests for HAVING clause."""

    def it_returns_statement() -> None:
        """Should return a Statement object."""
        source = Source(name="orders")
        statement = (
            source.select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.col("5"))
        )
        assert isinstance(statement, Statement)

    def it_renders_having_with_single_condition(render_config: vw.RenderConfig) -> None:
        """Should render HAVING with single condition."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.col("5"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING (COUNT(*) > 5)",
            params={},
        )

    def it_renders_having_with_multiple_conditions(render_config: vw.RenderConfig) -> None:
        """Should render HAVING with multiple conditions combined with AND."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.col("5"), vw.col("SUM(total)") > vw.col("1000"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id HAVING (COUNT(*) > 5) AND (SUM(total) > 1000)",
            params={},
        )

    def it_renders_having_with_parameters(render_config: vw.RenderConfig) -> None:
        """Should render HAVING with parameterized values."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.param("min_orders", 5))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING (COUNT(*) > :min_orders)",
            params={"min_orders": 5},
        )

    def it_chains_multiple_having_calls(render_config: vw.RenderConfig) -> None:
        """Should support chaining multiple having() calls."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"), vw.col("SUM(total)"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.col("5"))
            .having(vw.col("SUM(total)") > vw.col("1000"))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id HAVING (COUNT(*) > 5) AND (SUM(total) > 1000)",
            params={},
        )

    def it_renders_full_chain(render_config: vw.RenderConfig) -> None:
        """Should render WHERE, GROUP BY, and HAVING together."""
        result = (
            Source(name="orders")
            .select(vw.col("customer_id"), vw.col("COUNT(*)"))
            .where(vw.col("status") == vw.col("'completed'"))
            .group_by(vw.col("customer_id"))
            .having(vw.col("COUNT(*)") > vw.param("min_orders", 5))
            .render(config=render_config)
        )
        assert result == vw.RenderResult(
            sql="SELECT customer_id, COUNT(*) FROM orders WHERE (status = 'completed') GROUP BY customer_id HAVING (COUNT(*) > :min_orders)",
            params={"min_orders": 5},
        )
