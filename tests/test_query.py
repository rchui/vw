"""Tests for vw.query module."""

import vw
from vw.query import InnerJoin, Source, Statement


def describe_source():
    """Tests for Source class."""

    def it_renders_source_name():
        """Should render Source as its name."""
        source = Source("products")
        assert source.__vw_render__() == "products"

    def describe_col():
        """Tests for Source.col() method."""

        def it_returns_qualified_column():
            """Should return Column with source name prefix."""
            source = Source("users")
            column = source.col("id")
            assert column.__vw_render__() == "users.id"

        def it_returns_column_equal_to_manually_constructed():
            """Should return Column equal to manually constructed qualified column."""
            source = Source("orders")
            assert source.col("user_id") == vw.Column("orders.user_id")

        def it_works_with_different_column_names():
            """Should qualify any column name."""
            source = Source("orders")
            assert source.col("user_id").__vw_render__() == "orders.user_id"
            assert source.col("total").__vw_render__() == "orders.total"

    def describe_select():
        """Tests for Source.select() method."""

        def it_returns_statement():
            """Should return a Statement object."""
            source = Source("users")
            statement = source.select(vw.col("*"))
            assert isinstance(statement, Statement)

        def it_creates_statement_with_source_and_columns():
            """Should create Statement with correct source and columns."""
            source = Source("orders")
            col1 = vw.col("id")
            col2 = vw.col("name")
            statement = source.select(col1, col2)
            assert statement.source is source
            assert statement.columns == [col1, col2]


def describe_statement():
    """Tests for Statement class."""

    def describe_render():
        """Tests for Statement.render() method."""

        def it_renders_select_star():
            """Should render SELECT * FROM table."""
            source = Source("users")
            statement = Statement(source=source, columns=[vw.col("*")])
            assert statement.render() == "SELECT * FROM users"

        def it_renders_single_column():
            """Should render SELECT column FROM table."""
            source = Source("users")
            statement = Statement(source=source, columns=[vw.col("id")])
            assert statement.render() == "SELECT id FROM users"

        def it_renders_multiple_columns():
            """Should render SELECT col1, col2 FROM table."""
            source = Source("users")
            statement = Statement(source=source, columns=[vw.col("id"), vw.col("name"), vw.col("email")])
            assert statement.render() == "SELECT id, name, email FROM users"

    def it_vw_render_calls_render():
        """Should delegate __vw_render__ to render()."""
        source = Source("products")
        statement = Statement(source=source, columns=[vw.col("*")])
        assert statement.__vw_render__() == statement.render()


def describe_inner_join():
    """Tests for InnerJoin class."""

    def it_renders_inner_join_without_condition():
        """Should render INNER JOIN without ON clause."""
        users = Source("users")
        orders = Source("orders")
        join = InnerJoin(right=orders)
        assert join.__vw_render__() == "INNER JOIN orders"

    def it_renders_inner_join_with_single_condition():
        """Should render INNER JOIN with ON clause."""
        users = Source("users")
        orders = Source("orders")
        condition = users.col("id") == orders.col("user_id")
        join = InnerJoin(right=orders, on=[condition])
        assert join.__vw_render__() == "INNER JOIN orders ON users.id = orders.user_id"

    def it_renders_inner_join_with_multiple_conditions():
        """Should render INNER JOIN with multiple conditions combined with AND."""
        users = Source("users")
        orders = Source("orders")
        condition1 = users.col("id") == orders.col("user_id")
        condition2 = users.col("status") == vw.col("'active'")
        join = InnerJoin(right=orders, on=[condition1, condition2])
        assert join.__vw_render__() == "INNER JOIN orders ON users.id = orders.user_id AND users.status = 'active'"


def describe_join_accessor():
    """Tests for JoinAccessor class."""

    def it_creates_source_with_inner_join():
        """Should create a new Source with inner join."""
        users = Source("users")
        orders = Source("orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        assert isinstance(joined, Source)
        assert len(joined.joins) == 1
        assert isinstance(joined.joins[0], InnerJoin)

    def it_chains_multiple_joins():
        """Should support chaining multiple joins."""
        users = Source("users")
        orders = Source("orders")
        products = Source("products")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        joined = joined.join.inner(products, on=[orders.col("product_id") == products.col("id")])
        assert len(joined.joins) == 2
        assert joined.__vw_render__() == "users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id"


def describe_source_with_joins():
    """Tests for Source rendering with joins."""

    def it_renders_source_with_single_join():
        """Should render source with INNER JOIN."""
        users = Source("users")
        orders = Source("orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        assert joined.__vw_render__() == "users INNER JOIN orders ON users.id = orders.user_id"

    def it_renders_select_statement_with_join():
        """Should render SELECT statement with join."""
        users = Source("users")
        orders = Source("orders")
        joined = users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        statement = joined.select(vw.col("*"))
        assert statement.render() == "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
