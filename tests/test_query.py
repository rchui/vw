"""Tests for vw.query module."""

import vw
from vw.query import Source, Statement


def describe_source():
    """Tests for Source class."""

    def it_renders_source_name():
        """Should render Source as its name."""
        source = Source("products")
        assert source.__vw_render__() == "products"

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
