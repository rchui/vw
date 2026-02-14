"""Tests for core Star state."""

from vw.core.render import SQL
from vw.core.states import Reference, Star, Statement
from vw.postgres import ref, render


def describe_star_state() -> None:
    def it_creates_unqualified_star_without_source() -> None:
        """Star() should create unqualified star without source."""
        star = Star()
        assert star.source is None
        assert star.alias is None

    def it_creates_qualified_star_with_reference_source() -> None:
        """Star(source=Reference(...)) should create qualified star."""
        source = Reference(name="users")
        star = Star(source=source)
        assert star.source == source
        assert star.alias is None

    def it_creates_qualified_star_with_statement_source() -> None:
        """Star(source=Statement(...)) should create qualified star with statement."""
        source = Statement(source=Reference(name="users"), alias="u")
        star = Star(source=source)
        assert star.source == source
        assert star.alias is None

    def it_creates_star_with_alias() -> None:
        """Star(alias=...) should create star with alias."""
        star = Star(alias="all_cols")
        assert star.source is None
        assert star.alias == "all_cols"

    def it_creates_qualified_star_with_alias() -> None:
        """Star(source=..., alias=...) should create qualified star with alias."""
        source = Reference(name="users", alias="u")
        star = Star(source=source, alias="all_cols")
        assert star.source == source
        assert star.alias == "all_cols"

    def it_is_an_expr_subclass() -> None:
        """Star should be an Expr subclass."""
        from vw.core.states import Expr

        star = Star()
        assert isinstance(star, Expr)

    def it_is_immutable() -> None:
        """Star should be immutable (frozen dataclass)."""
        import pytest

        star = Star()
        with pytest.raises(AttributeError):
            star.alias = "new_alias"  # type: ignore


def describe_star_in_select() -> None:
    def it_renders_unqualified_star() -> None:
        """Unqualified star should render as *."""
        star = Star()
        # We need to wrap it in an Expression to render with postgres
        from vw.core.base import Factories
        from vw.postgres.base import Expression

        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="*", params={})

    def it_renders_qualified_star_with_reference() -> None:
        """Qualified star with Reference should render as table.*."""
        source = Reference(name="users", alias="u")
        star = Star(source=source)
        from vw.core.base import Factories
        from vw.postgres.base import Expression

        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="u.*", params={})

    def it_renders_star_with_alias() -> None:
        """Star with alias should render as * AS alias."""
        star = Star(alias="all_cols")
        from vw.core.base import Factories
        from vw.postgres.base import Expression

        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="* AS all_cols", params={})

    def it_works_in_select_query() -> None:
        """Star property always qualifies with source name."""
        users = ref("users")
        q = users.select(users.star())
        result = render(q)
        # The star property always passes the source, so it renders as qualified
        assert result == SQL(query="SELECT users.* FROM users", params={})

    def it_works_with_qualified_source() -> None:
        """Star should work with qualified source in SELECT."""
        users = ref("users").alias("u")
        q = users.select(users.star())
        result = render(q)
        assert result == SQL(query="SELECT u.* FROM users AS u", params={})
