"""Tests for col() function and Column rendering."""

from vw.postgres import col, ref, render


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

