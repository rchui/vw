"""Tests for source() function and Source rendering."""

from vw.postgres import col, render, source


def describe_source() -> None:
    def it_creates_source() -> None:
        """source() should create a RowSet with Source state."""
        s = source("users")
        assert s.state.name == "users"
        assert s.state.alias is None

    def it_renders_with_from() -> None:
        """Source should render with FROM prefix."""
        s = source("users")
        assert render(s) == "FROM users"

    def describe_alias() -> None:
        def it_sets_alias_on_source() -> None:
            """alias() should set the alias on the Source."""
            s = source("users").alias("u")
            assert s.state.alias == "u"

        def it_renders_with_alias() -> None:
            """Aliased source should render with AS."""
            s = source("users").alias("u")
            assert render(s) == "FROM users AS u"

        def it_works_before_select() -> None:
            """Alias can be set before select()."""
            s = source("users").alias("u").select(col("id"))
            assert render(s) == "SELECT id FROM users AS u"


def describe_select() -> None:
    def it_transforms_source_to_statement() -> None:
        """select() should transform Source to Statement."""
        from vw.core.states import Source, Statement

        s = source("users")
        assert isinstance(s.state, Source)

        q = s.select(col("id"))
        assert isinstance(q.state, Statement)

    def it_renders_single_column() -> None:
        """Single column SELECT should render correctly."""
        q = source("users").select(col("id"))
        assert render(q) == "SELECT id FROM users"

    def it_renders_multiple_columns() -> None:
        """Multiple columns should render correctly."""
        q = source("users").select(col("id"), col("name"), col("email"))
        assert render(q) == "SELECT id, name, email FROM users"

    def it_replaces_columns_on_second_select() -> None:
        """Second select() should replace columns, not append."""
        q = source("users").select(col("id")).select(col("name"))
        assert render(q) == "SELECT name FROM users"

    def it_preserves_source_alias() -> None:
        """select() should preserve the source alias."""
        q = source("users").alias("u").select(col("id"))
        assert render(q) == "SELECT id FROM users AS u"


def describe_statement_alias() -> None:
    def it_sets_alias_on_statement() -> None:
        """alias() on Statement should set statement alias."""
        from vw.core.states import Statement

        q = source("users").select(col("id")).alias("subq")
        assert isinstance(q.state, Statement)
        assert q.state.alias == "subq"

    def it_does_not_render_top_level_statement_alias() -> None:
        """Top-level statement alias should not render (only for subqueries)."""
        q = source("users").select(col("id")).alias("subq")
        assert render(q) == "SELECT id FROM users"
