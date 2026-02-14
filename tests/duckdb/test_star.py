"""Tests for DuckDB Star rendering with extensions."""

from vw.core.render import SQL
from vw.core.states import CTE, Reference, Values
from vw.duckdb import col, ref, render


def describe_star_accessor() -> None:
    def it_creates_star_accessor_from_rowset() -> None:
        """RowSet.star should return StarAccessor."""
        from vw.duckdb.star import StarAccessor

        users = ref("users")
        assert isinstance(users.star, StarAccessor)

    def it_star_accessor_is_callable() -> None:
        """StarAccessor should be callable."""
        users = ref("users")
        assert callable(users.star)

    def it_calling_star_returns_expression() -> None:
        """Calling star() should return Expression."""
        from vw.duckdb.base import Expression

        users = ref("users")
        result = users.star()
        assert isinstance(result, Expression)

    def it_exclude_returns_star_exclude() -> None:
        """star.exclude() should return StarExclude."""
        from vw.duckdb.states import StarExclude

        users = ref("users")
        exclude = users.star.exclude(col("password"))
        assert isinstance(exclude, StarExclude)
        assert len(exclude.columns) == 1

    def it_exclude_accepts_multiple_columns() -> None:
        """star.exclude() should accept multiple columns."""
        from vw.duckdb.states import StarExclude

        users = ref("users")
        exclude = users.star.exclude(col("password"), col("secret"))
        assert isinstance(exclude, StarExclude)
        assert len(exclude.columns) == 2

    def it_replace_returns_star_replace() -> None:
        """star.replace() should return StarReplace."""
        from vw.duckdb.states import StarReplace

        users = ref("users")
        replace = users.star.replace(name=col("first_name"))
        assert isinstance(replace, StarReplace)
        assert "name" in replace.replacements

    def it_replace_accepts_multiple_replacements() -> None:
        """star.replace() should accept multiple replacements."""
        from vw.duckdb.states import StarReplace

        users = ref("users")
        replace = users.star.replace(name=col("first_name"), age=col("years"))
        assert isinstance(replace, StarReplace)
        assert len(replace.replacements) == 2
        assert "name" in replace.replacements
        assert "age" in replace.replacements


def describe_star_basic_rendering() -> None:
    def it_renders_star_with_reference_and_no_alias() -> None:
        """Star property always qualifies with source (table name if no alias)."""
        users = ref("users")
        result = render(users.star())
        assert result == SQL(query="users.*", params={})

    def it_renders_qualified_star_with_alias() -> None:
        """Qualified star with alias should render as alias.*."""
        users = ref("users").alias("u")
        result = render(users.star())
        assert result == SQL(query="u.*", params={})

    def it_renders_qualified_star_without_alias() -> None:
        """Qualified star without alias should use table name."""
        from vw.core.base import Factories
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        source = Reference(name="users")
        star = Star(source=source)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="users.*", params={})

    def it_renders_star_with_statement_alias() -> None:
        """Star with Statement alias should render as alias.*."""
        subq = ref("users").select(col("id")).alias("subq")
        result = render(subq.star())
        assert result == SQL(query="subq.*", params={})

    def it_renders_star_with_cte_source() -> None:
        """Star with CTE source should render as cte_name.*."""
        from vw.core.base import Factories
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        cte_state = CTE(
            name="user_stats",
            source=Reference(name="users"),
            columns=(),
        )
        star = Star(source=cte_state)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="user_stats.*", params={})


def describe_star_exclude_rendering() -> None:
    def it_renders_star_with_exclude() -> None:
        """Star with EXCLUDE should render correctly."""
        users = ref("users")
        result = render(users.star(users.star.exclude(col("password"))))
        assert result == SQL(query="users.* EXCLUDE (password)", params={})

    def it_renders_star_with_multiple_excludes() -> None:
        """Star with multiple EXCLUDE columns should render correctly."""
        users = ref("users")
        result = render(users.star(users.star.exclude(col("password"), col("secret"))))
        assert result == SQL(query="users.* EXCLUDE (password, secret)", params={})

    def it_renders_qualified_star_with_exclude() -> None:
        """Qualified star with EXCLUDE should render correctly."""
        users = ref("users").alias("u")
        result = render(users.star(users.star.exclude(col("password"))))
        assert result == SQL(query="u.* EXCLUDE (password)", params={})


def describe_star_replace_rendering() -> None:
    def it_renders_star_with_replace() -> None:
        """Star with REPLACE should render correctly."""
        users = ref("users")
        result = render(users.star(users.star.replace(name=col("first_name"))))
        assert result == SQL(query="users.* REPLACE (first_name AS name)", params={})

    def it_renders_star_with_multiple_replaces() -> None:
        """Star with multiple REPLACE expressions should render correctly."""
        users = ref("users")
        result = render(users.star(users.star.replace(name=col("first_name"), age=col("years"))))
        # Note: dict ordering in Python 3.7+ is insertion order
        assert "REPLACE" in result.query
        assert "first_name AS name" in result.query
        assert "years AS age" in result.query
        assert result.params == {}

    def it_renders_qualified_star_with_replace() -> None:
        """Qualified star with REPLACE should render correctly."""
        users = ref("users").alias("u")
        result = render(users.star(users.star.replace(name=col("first_name"))))
        assert result == SQL(query="u.* REPLACE (first_name AS name)", params={})


def describe_star_combined_modifiers() -> None:
    def it_renders_star_with_exclude_and_replace() -> None:
        """Star with both EXCLUDE and REPLACE should render correctly."""
        users = ref("users")
        result = render(
            users.star(
                users.star.exclude(col("password")),
                users.star.replace(name=col("first_name")),
            )
        )
        assert result == SQL(query="users.* EXCLUDE (password) REPLACE (first_name AS name)", params={})

    def it_renders_qualified_star_with_both_modifiers() -> None:
        """Qualified star with both EXCLUDE and REPLACE should render correctly."""
        users = ref("users").alias("u")
        result = render(
            users.star(
                users.star.exclude(col("password"), col("secret")),
                users.star.replace(name=col("full_name")),
            )
        )
        assert result == SQL(query="u.* EXCLUDE (password, secret) REPLACE (full_name AS name)", params={})

    def it_preserves_modifier_order() -> None:
        """Star should preserve modifier order."""
        users = ref("users")
        # EXCLUDE then REPLACE
        result1 = render(
            users.star(
                users.star.exclude(col("a")),
                users.star.replace(b=col("c")),
            )
        )
        assert result1.query == "users.* EXCLUDE (a) REPLACE (c AS b)"

        # REPLACE then EXCLUDE
        result2 = render(
            users.star(
                users.star.replace(b=col("c")),
                users.star.exclude(col("a")),
            )
        )
        assert result2.query == "users.* REPLACE (c AS b) EXCLUDE (a)"

    def it_renders_star_with_alias_and_modifiers() -> None:
        """Star with alias and modifiers should render correctly."""
        from vw.core.base import Factories
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        users = ref("users")
        star = Star(
            source=users.state,
            modifiers=(users.star.exclude(col("password")),),
            alias="all_cols",
        )
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))
        result = render(expr)
        assert result == SQL(query="users.* EXCLUDE (password) AS all_cols", params={})


def describe_star_error_cases() -> None:
    def it_raises_error_for_unsupported_source_type() -> None:
        """Star with unsupported source type should raise TypeError."""
        import pytest

        from vw.core.base import Factories
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        # Values is not a supported source type
        values_source = Values(rows=({"id": 1},), alias="v")
        star = Star(source=values_source)
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))

        with pytest.raises(TypeError, match="Unsupported Star source type: Values"):
            render(expr)

    def it_raises_error_for_unsupported_modifier_type() -> None:
        """Star with unsupported modifier type should raise TypeError."""
        import pytest

        from vw.core.base import Factories
        from vw.duckdb.base import Expression
        from vw.duckdb.states import Star

        # Create a fake modifier class
        class FakeModifier:
            pass

        users = ref("users")
        star = Star(source=users.state, modifiers=(FakeModifier(),))  # type: ignore
        expr = Expression(state=star, factories=Factories(expr=Expression, rowset=ref("users").__class__))

        with pytest.raises(TypeError, match="Unsupported Star modifier type: FakeModifier"):
            render(expr)


def describe_star_in_select() -> None:
    def it_works_in_basic_select() -> None:
        """Star should work in basic SELECT."""
        users = ref("users")
        q = users.select(users.star())
        result = render(q)
        assert result == SQL(query="SELECT users.* FROM users", params={})

    def it_works_with_exclude_in_select() -> None:
        """Star with EXCLUDE should work in SELECT."""
        users = ref("users")
        q = users.select(users.star(users.star.exclude(col("password"))))
        result = render(q)
        assert result == SQL(query="SELECT users.* EXCLUDE (password) FROM users", params={})

    def it_works_with_replace_in_select() -> None:
        """Star with REPLACE should work in SELECT."""
        users = ref("users")
        q = users.select(users.star(users.star.replace(name=col("first_name"))))
        result = render(q)
        assert result == SQL(query="SELECT users.* REPLACE (first_name AS name) FROM users", params={})

    def it_works_with_both_modifiers_in_select() -> None:
        """Star with both modifiers should work in SELECT."""
        users = ref("users").alias("u")
        q = users.select(
            users.star(
                users.star.exclude(col("password")),
                users.star.replace(name=col("full_name")),
            )
        )
        result = render(q)
        assert result == SQL(
            query="SELECT u.* EXCLUDE (password) REPLACE (full_name AS name) FROM users AS u", params={}
        )

    def it_works_alongside_other_columns() -> None:
        """Star with modifiers should work alongside other columns."""
        users = ref("users")
        q = users.select(users.star(users.star.exclude(col("password"))), col("created_at"))
        result = render(q)
        assert result == SQL(query="SELECT users.* EXCLUDE (password), created_at FROM users", params={})

    def it_works_in_join_query() -> None:
        """Star with modifiers should work in JOIN queries."""
        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        q = users.select(users.star(users.star.exclude(col("password"))), col("o.total")).join.inner(
            orders, on=[(col("u.id") == col("o.user_id"))]
        )
        result = render(q)
        assert result == SQL(
            query="SELECT u.* EXCLUDE (password), o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)",
            params={},
        )

    def it_works_with_statement_source() -> None:
        """Star should work with Statement source (aliased subquery)."""
        subq = ref("users").select(col("id")).alias("subq")
        result = render(subq.star())
        assert result == SQL(query="subq.*", params={})
