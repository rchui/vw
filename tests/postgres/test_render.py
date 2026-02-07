"""Tests for render() function."""

import pytest

from vw.postgres import col, ref, render


def describe_render() -> None:
    def it_renders_ref() -> None:
        """Should render Reference with FROM."""
        s = ref("users")
        result = render(s)
        assert result.query == "FROM users"
        assert result.params == {}

    def it_renders_statement() -> None:
        """Should render Statement with SELECT."""
        q = ref("users").select(col("id"))
        result = render(q)
        assert result.query == "SELECT id FROM users"
        assert result.params == {}

    def it_renders_expression() -> None:
        """Should render Expression (Column)."""
        c = col("id")
        result = render(c)
        assert result.query == "id"
        assert result.params == {}

    def it_raises_on_unknown_type() -> None:
        """Should raise TypeError for unknown state types."""
        from vw.core.render import RenderConfig, RenderContext
        from vw.postgres.render import ParamStyle, render_state

        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        with pytest.raises(TypeError, match="Unknown state type"):
            render_state(42, ctx)


def describe_render_source() -> None:
    def it_renders_simple_reference() -> None:
        """Simple reference should render as name."""
        from vw.core.render import RenderConfig, RenderContext
        from vw.core.states import Reference
        from vw.postgres.render import ParamStyle, render_source

        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        s = Reference(name="users")
        assert render_source(s, ctx) == "users"

    def it_renders_aliased_reference() -> None:
        """Aliased reference should render with AS."""
        from vw.core.render import RenderConfig, RenderContext
        from vw.core.states import Reference
        from vw.postgres.render import ParamStyle, render_source

        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        s = Reference(name="users", alias="u")
        assert render_source(s, ctx) == "users AS u"


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
        """Statement with Reference should render correctly."""
        from vw.core.render import RenderConfig, RenderContext
        from vw.core.states import Reference, Statement
        from vw.postgres.render import ParamStyle, render_statement

        ctx = RenderContext(config=RenderConfig(param_style=ParamStyle.DOLLAR))
        stmt = Statement(source=Reference(name="users"), columns=())
        assert render_statement(stmt, ctx) == "FROM users"

    def it_renders_statement_with_columns() -> None:
        """Statement with columns should render SELECT."""
        q = ref("users").select(col("id"), col("name"))
        result = render(q)
        assert result.query == "SELECT id, name FROM users"
        assert result.params == {}

    def it_renders_statement_with_aliased_reference() -> None:
        """Statement with aliased reference should include alias."""
        q = ref("users").alias("u").select(col("id"))
        result = render(q)
        assert result.query == "SELECT id FROM users AS u"
        assert result.params == {}
