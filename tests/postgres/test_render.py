"""Tests for render() function."""

import pytest

from vw.postgres import col, render, source


def describe_render() -> None:
    def it_renders_source() -> None:
        """Should render Source with FROM."""
        s = source("users")
        assert render(s) == "FROM users"

    def it_renders_statement() -> None:
        """Should render Statement with SELECT."""
        q = source("users").select(col("id"))
        assert render(q) == "SELECT id FROM users"

    def it_renders_expression() -> None:
        """Should render Expression (Column)."""
        c = col("id")
        assert render(c) == "id"

    def it_raises_on_unknown_type() -> None:
        """Should raise TypeError for unknown state types."""
        from vw.postgres.render import render_state

        with pytest.raises(TypeError, match="Unknown state type"):
            render_state(42)


def describe_render_source() -> None:
    def it_renders_simple_source() -> None:
        """Simple source should render as name."""
        from vw.core.states import Source
        from vw.postgres.render import render_source

        s = Source(name="users")
        assert render_source(s) == "users"

    def it_renders_aliased_source() -> None:
        """Aliased source should render with AS."""
        from vw.core.states import Source
        from vw.postgres.render import render_source

        s = Source(name="users", alias="u")
        assert render_source(s) == "users AS u"


def describe_render_column() -> None:
    def it_renders_simple_column() -> None:
        """Simple column should render as name."""
        from vw.core.states import Column
        from vw.postgres.render import render_column

        c = Column(name="id")
        assert render_column(c) == "id"

    def it_renders_aliased_column() -> None:
        """Aliased column should render with AS."""
        from vw.core.states import Column
        from vw.postgres.render import render_column

        c = Column(name="id", alias="user_id")
        assert render_column(c) == "id AS user_id"


def describe_render_statement() -> None:
    def it_renders_statement_with_source() -> None:
        """Statement with Source should render correctly."""
        from vw.core.states import Source, Statement
        from vw.postgres.render import render_statement

        stmt = Statement(source=Source(name="users"), columns=())
        assert render_statement(stmt) == "FROM users"

    def it_renders_statement_with_columns() -> None:
        """Statement with columns should render SELECT."""
        q = source("users").select(col("id"), col("name"))
        result = render(q)
        assert result == "SELECT id, name FROM users"

    def it_renders_statement_with_aliased_source() -> None:
        """Statement with aliased source should include alias."""
        q = source("users").alias("u").select(col("id"))
        assert render(q) == "SELECT id FROM users AS u"
