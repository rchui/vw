"""Tests for PostgreSQL Star rendering."""

from vw.core.render import SQL
from vw.core.states import CTE, Reference, Values
from vw.postgres import col, param, ref, render


def describe_star_rendering() -> None:
    def it_renders_star_with_reference_and_no_alias() -> None:
        """Star property always qualifies with source (table name if no alias)."""
        users = ref("users")
        result = render(users.star())
        assert result == SQL(query="users.*", params={})

    def it_renders_qualified_star_with_reference() -> None:
        """Star with Reference source should render as table.*."""
        users = ref("users").alias("u")
        result = render(users.star())
        assert result == SQL(query="u.*", params={})

    def it_renders_qualified_star_without_alias() -> None:
        """Star with Reference (no alias) should use table name."""
        users = ref("users")
        # Manually create a qualified star
        from vw.core.base import Factories
        from vw.core.states import Star
        from vw.postgres.base import Expression

        star = Star(source=users.state)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="users.*", params={})

    def it_renders_star_with_alias() -> None:
        """Star with alias should render as * AS alias."""
        from vw.core.base import Factories
        from vw.core.states import Star
        from vw.postgres.base import Expression

        star = Star(alias="all_cols")
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="* AS all_cols", params={})

    def it_renders_qualified_star_with_alias() -> None:
        """Qualified star with alias should render as table.* AS alias."""
        from vw.core.base import Factories
        from vw.core.states import Star
        from vw.postgres.base import Expression

        users = ref("users").alias("u")
        star = Star(source=users.state, alias="all_cols")
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="u.* AS all_cols", params={})

    def it_renders_star_with_cte_source() -> None:
        """Star with CTE source should use CTE name for qualification."""
        from vw.core.base import Factories
        from vw.core.states import Star
        from vw.postgres.base import Expression

        # Create a minimal CTE state manually to test render_star
        # CTE extends Statement, so we need to provide Statement fields
        cte_state = CTE(
            name="user_stats",
            source=Reference(name="users"),
            columns=(),
        )
        star = Star(source=cte_state)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="user_stats.*", params={})

    def it_renders_star_with_statement_alias() -> None:
        """Star with Statement alias should render as alias.*."""
        subq = ref("users").select(col("id"), col("name")).alias("subq")
        result = render(subq.star())
        assert result == SQL(query="subq.*", params={})

    def it_raises_error_for_unsupported_source_type() -> None:
        """Star with unsupported source type should raise TypeError."""
        import pytest

        from vw.core.base import Factories
        from vw.core.states import Star
        from vw.postgres.base import Expression

        # Values is not a supported source type for Star
        values_source = Values(rows=({"id": 1},), alias="v")
        star = Star(source=values_source)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))

        with pytest.raises(TypeError, match="Unsupported Star source type: Values"):
            render(expr)


def describe_star_in_select() -> None:
    def it_works_in_basic_select() -> None:
        """Star property qualifies with source name."""
        users = ref("users")
        q = users.select(users.star())
        result = render(q)
        assert result == SQL(query="SELECT users.* FROM users", params={})

    def it_works_with_qualified_source() -> None:
        """Star should work with qualified source."""
        users = ref("users").alias("u")
        q = users.select(users.star())
        result = render(q)
        assert result == SQL(query="SELECT u.* FROM users AS u", params={})

    def it_works_with_other_columns() -> None:
        """Star should work alongside other columns."""
        users = ref("users").alias("u")
        q = users.select(users.star(), col("email"))
        result = render(q)
        assert result == SQL(query="SELECT u.*, email FROM users AS u", params={})

    def it_works_in_join() -> None:
        """Star should work in JOIN queries."""
        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(users.star(), col("o.total")).join.inner(orders, on=[(col("u.id") == col("o.user_id"))])
        result = render(q)
        assert result == SQL(
            query="SELECT u.*, o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)", params={}
        )

    def it_works_with_where_clause() -> None:
        """Star should work with WHERE clause."""
        users = ref("users")
        q = users.select(users.star()).where(col("active") == param("is_active", True))
        result = render(q)
        assert result == SQL(query="SELECT users.* FROM users WHERE active = $is_active", params={"is_active": True})

    def it_works_with_statement_source() -> None:
        """Star should work with Statement source (aliased subquery)."""
        subq = ref("users").select(col("id"), col("name")).alias("subq")
        result = render(subq.star())
        assert result == SQL(query="subq.*", params={})
