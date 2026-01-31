"""Tests for col() function and Column rendering."""

from vw.postgres import col, render, source


def describe_col() -> None:
    def it_creates_column_expression() -> None:
        """col() should create an Expression with Column state."""
        from vw.core.states import Column

        c = col("id")
        assert isinstance(c.state, Column)
        assert c.state.name == "id"
        assert c.state.alias is None

    def it_renders_simple_column() -> None:
        """Simple column should render as name."""
        c = col("id")
        result = render(c)
        assert result.query == "id"
        assert result.params == {}

    def it_renders_qualified_column() -> None:
        """Qualified column should render with prefix."""
        c = col("users.id")
        result = render(c)
        assert result.query == "users.id"
        assert result.params == {}

    def it_renders_in_select() -> None:
        """Column should render correctly in SELECT."""
        q = source("users").select(col("id"))
        result = render(q)
        assert result.query == "SELECT id FROM users"
        assert result.params == {}

    def it_renders_multiple_columns_in_select() -> None:
        """Multiple columns should render separated by commas."""
        q = source("users").select(col("id"), col("name"), col("email"))
        result = render(q)
        assert result.query == "SELECT id, name, email FROM users"
        assert result.params == {}
