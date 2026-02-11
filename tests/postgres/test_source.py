"""Tests for ref() function and Reference rendering."""

from vw.core.states import Reference
from vw.postgres import col, param, raw, ref, render


def describe_ref() -> None:
    def it_creates_reference() -> None:
        """ref() should create a RowSet with Reference state."""
        s = ref("users")
        assert isinstance(s.state, Reference)
        assert s.state.name == "users"
        assert s.state.alias is None

    def it_renders_with_from() -> None:
        """Reference should render with FROM prefix."""
        s = ref("users")
        result = render(s)
        assert result.query == "FROM users"
        assert result.params == {}

    def describe_alias() -> None:
        def it_sets_alias_on_reference() -> None:
            """alias() should set the alias on the Reference."""
            s = ref("users").alias("u")
            assert s.state.alias == "u"

        def it_renders_with_alias() -> None:
            """Aliased reference should render with AS."""
            s = ref("users").alias("u")
            result = render(s)
            assert result.query == "FROM users AS u"
            assert result.params == {}

        def it_works_before_select() -> None:
            """Alias can be set before select()."""
            s = ref("users").alias("u").select(col("id"))
            result = render(s)
            assert result.query == "SELECT id FROM users AS u"
            assert result.params == {}


def describe_select() -> None:
    def it_transforms_reference_to_statement() -> None:
        """select() should transform Reference to Statement."""
        from vw.core.states import Reference, Statement

        s = ref("users")
        assert isinstance(s.state, Reference)

        q = s.select(col("id"))
        assert isinstance(q.state, Statement)

    def it_renders_single_column() -> None:
        """Single column SELECT should render correctly."""
        q = ref("users").select(col("id"))
        result = render(q)
        assert result.query == "SELECT id FROM users"
        assert result.params == {}

    def it_renders_multiple_columns() -> None:
        """Multiple columns should render correctly."""
        q = ref("users").select(col("id"), col("name"), col("email"))
        result = render(q)
        assert result.query == "SELECT id, name, email FROM users"
        assert result.params == {}

    def it_replaces_columns_on_second_select() -> None:
        """Second select() should replace columns, not append."""
        q = ref("users").select(col("id")).select(col("name"))
        result = render(q)
        assert result.query == "SELECT name FROM users"
        assert result.params == {}

    def it_preserves_reference_alias() -> None:
        """select() should preserve the reference alias."""
        q = ref("users").alias("u").select(col("id"))
        result = render(q)
        assert result.query == "SELECT id FROM users AS u"
        assert result.params == {}


def describe_statement_alias() -> None:
    def it_sets_alias_on_statement() -> None:
        """alias() on Statement should set statement alias."""
        from vw.core.states import Statement

        q = ref("users").select(col("id")).alias("subq")
        assert isinstance(q.state, Statement)
        assert q.state.alias == "subq"

    def it_does_not_render_top_level_statement_alias() -> None:
        """Top-level statement alias should not render (only for subqueries)."""
        q = ref("users").select(col("id")).alias("subq")
        result = render(q)
        assert result.query == "SELECT id FROM users"
        assert result.params == {}


def describe_modifiers() -> None:
    def describe_table_level() -> None:
        def it_renders_tablesample() -> None:
            """Modifier on table should render between name and alias."""
            q = ref("users").modifiers(raw.expr("TABLESAMPLE SYSTEM(5)")).select(col("*"))
            result = render(q)
            assert result.query == "SELECT * FROM users TABLESAMPLE SYSTEM(5)"

        def it_renders_tablesample_with_parameter() -> None:
            """Modifier can use parameters."""
            q = (
                ref("users")
                .modifiers(raw.expr("TABLESAMPLE SYSTEM({pct})", pct=param("sample_pct", 10)))
                .select(col("id"))
            )
            result = render(q)
            assert result.query == "SELECT id FROM users TABLESAMPLE SYSTEM($sample_pct)"
            assert result.params == {"sample_pct": 10}

        def it_renders_partition_clause() -> None:
            """Partition modifier renders correctly."""
            q = ref("sales").modifiers(raw.expr("PARTITION (p2023, p2024)")).select(col("*"))
            result = render(q)
            assert result.query == "SELECT * FROM sales PARTITION (p2023, p2024)"

        def it_renders_modifier_before_alias() -> None:
            """Modifier should render before alias."""
            q = ref("users").modifiers(raw.expr("TABLESAMPLE BERNOULLI(5)")).alias("u").select(col("u.id"))
            result = render(q)
            assert result.query == "SELECT u.id FROM users TABLESAMPLE BERNOULLI(5) AS u"

        def it_accumulates_multiple_modifiers() -> None:
            """Multiple modifiers accumulate in order."""
            q = (
                ref("events")
                .modifiers(raw.expr("TABLESAMPLE SYSTEM(10)"), raw.expr("REPEATABLE(123)"))
                .select(col("*"))
            )
            result = render(q)
            assert result.query == "SELECT * FROM events TABLESAMPLE SYSTEM(10) REPEATABLE(123)"
