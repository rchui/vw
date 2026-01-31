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
        assert render(c) == "id"

    def it_renders_qualified_column() -> None:
        """Qualified column should render with prefix."""
        c = col("users.id")
        assert render(c) == "users.id"

    def it_renders_in_select() -> None:
        """Column should render correctly in SELECT."""
        q = source("users").select(col("id"))
        assert render(q) == "SELECT id FROM users"

    def it_renders_multiple_columns_in_select() -> None:
        """Multiple columns should render separated by commas."""
        q = source("users").select(col("id"), col("name"), col("email"))
        assert render(q) == "SELECT id, name, email FROM users"
