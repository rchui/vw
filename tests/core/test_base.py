"""Tests for core ANSI SQL RowSet methods.

This file tests the core RowSet methods that work across all SQL dialects:
- col() - column references
- star - star selection
- where() - WHERE clause
- group_by() - GROUP BY clause
- having() - HAVING clause
- order_by() - ORDER BY clause
- limit() - LIMIT clause
- offset() - OFFSET clause
- distinct() - DISTINCT modifier

PostgreSQL-specific features (fetch with WITH TIES, modifiers for row-level locking)
are tested in tests/postgres/test_base.py.
"""

from vw.postgres import col, ref, render


def describe_rowset() -> None:
    def describe_col() -> None:
        def it_creates_unqualified_column_without_alias() -> None:
            """col() should create unqualified column when reference has no alias."""
            s = ref("users")
            c = s.col("id")
            result = render(c)
            assert result.query == "id"
            assert result.params == {}

        def it_creates_qualified_column_with_alias() -> None:
            """col() should create qualified column when reference has alias."""
            s = ref("users").alias("u")
            c = s.col("id")
            result = render(c)
            assert result.query == "u.id"
            assert result.params == {}

        def it_works_in_select() -> None:
            """RowSet.col() should work in select()."""
            s = ref("users").alias("u")
            q = s.select(s.col("id"), s.col("name"))
            result = render(q)
            assert result.query == "SELECT u.id, u.name FROM users AS u"
            assert result.params == {}

        def it_works_in_where() -> None:
            """RowSet.col() should work in where()."""
            s = ref("users").alias("u")
            q = s.select(s.col("id")).where(s.col("active"))
            result = render(q)
            assert result.query == "SELECT u.id FROM users AS u WHERE u.active"
            assert result.params == {}

        def it_works_with_statement_alias() -> None:
            """col() should use Statement alias when available."""
            s = ref("users").select(col("id")).alias("subq")
            c = s.col("id")
            result = render(c)
            assert result.query == "subq.id"
            assert result.params == {}

    def describe_star() -> None:
        def it_creates_unqualified_star_without_alias() -> None:
            """star should create unqualified star when reference has no alias."""
            s = ref("users")
            result = render(s.star)
            assert result.query == "*"
            assert result.params == {}

        def it_creates_qualified_star_with_alias() -> None:
            """star should create qualified star when reference has alias."""
            s = ref("users").alias("u")
            result = render(s.star)
            assert result.query == "u.*"
            assert result.params == {}

        def it_works_in_select() -> None:
            """RowSet.star should work in select()."""
            s = ref("users").alias("u")
            q = s.select(s.star)
            result = render(q)
            assert result.query == "SELECT u.* FROM users AS u"
            assert result.params == {}

        def it_works_without_alias_in_select() -> None:
            """RowSet.star should work in select without alias."""
            s = ref("users")
            q = s.select(s.star)
            result = render(q)
            assert result.query == "SELECT * FROM users"
            assert result.params == {}

        def it_works_with_statement_alias() -> None:
            """star should use Statement alias when available."""
            s = ref("users").select(col("id")).alias("subq")
            result = render(s.star)
            assert result.query == "subq.*"
            assert result.params == {}

    def describe_where() -> None:
        def it_renders_where_clause() -> None:
            """WHERE clause should render correctly."""
            q = ref("users").select(col("id")).where(col("active"))
            result = render(q)
            assert result.query == "SELECT id FROM users WHERE active"
            assert result.params == {}

        def it_accumulates_multiple_conditions() -> None:
            """Multiple where() calls should accumulate conditions."""
            q = ref("users").select(col("id")).where(col("active")).where(col("verified"))
            result = render(q)
            assert result.query == "SELECT id FROM users WHERE active AND verified"
            assert result.params == {}

        def it_accepts_multiple_conditions_in_one_call() -> None:
            """where() should accept multiple conditions."""
            q = ref("users").select(col("id")).where(col("active"), col("verified"))
            result = render(q)
            assert result.query == "SELECT id FROM users WHERE active AND verified"
            assert result.params == {}

        def it_works_before_select() -> None:
            """where() can be called before select()."""
            q = ref("users").where(col("active")).select(col("id"))
            result = render(q)
            assert result.query == "SELECT id FROM users WHERE active"
            assert result.params == {}

        def it_works_without_select() -> None:
            """where() transforms Reference even without select()."""
            q = ref("users").where(col("active"))
            result = render(q)
            assert result.query == "FROM users WHERE active"
            assert result.params == {}

        def it_preserves_ref_alias() -> None:
            """where() should preserve reference alias."""
            q = ref("users").alias("u").select(col("id")).where(col("u.active"))
            result = render(q)
            assert result.query == "SELECT id FROM users AS u WHERE u.active"
            assert result.params == {}

        def it_chains_with_multiple_clauses() -> None:
            """where() should chain with other clauses."""
            q = ref("users").select(col("id"), col("name")).where(col("active")).limit(10)
            result = render(q)
            assert result.query == "SELECT id, name FROM users WHERE active LIMIT 10"
            assert result.params == {}

    def describe_group_by() -> None:
        def it_renders_group_by_clause() -> None:
            """GROUP BY clause should render correctly."""
            q = ref("orders").select(col("user_id")).group_by(col("user_id"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders GROUP BY user_id"
            assert result.params == {}

        def it_accepts_multiple_columns() -> None:
            """group_by() should accept multiple columns."""
            q = ref("orders").select(col("user_id"), col("product_id")).group_by(col("user_id"), col("product_id"))
            result = render(q)
            assert result.query == "SELECT user_id, product_id FROM orders GROUP BY user_id, product_id"
            assert result.params == {}

        def it_replaces_on_multiple_calls() -> None:
            """Second group_by() call should replace first."""
            q = ref("orders").select(col("product_id")).group_by(col("user_id")).group_by(col("product_id"))
            result = render(q)
            assert result.query == "SELECT product_id FROM orders GROUP BY product_id"
            assert result.params == {}

        def it_works_before_select() -> None:
            """group_by() can be called before select()."""
            q = ref("orders").group_by(col("user_id")).select(col("user_id"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders GROUP BY user_id"
            assert result.params == {}

        def it_preserves_ref_alias() -> None:
            """group_by() should preserve reference alias."""
            q = ref("orders").alias("o").select(col("o.user_id")).group_by(col("o.user_id"))
            result = render(q)
            assert result.query == "SELECT o.user_id FROM orders AS o GROUP BY o.user_id"
            assert result.params == {}

    def describe_having() -> None:
        def it_renders_having_clause() -> None:
            """HAVING clause should render correctly."""
            q = ref("orders").select(col("user_id")).group_by(col("user_id")).having(col("count"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders GROUP BY user_id HAVING count"
            assert result.params == {}

        def it_accumulates_multiple_conditions() -> None:
            """Multiple having() calls should accumulate conditions."""
            q = ref("orders").select(col("user_id")).group_by(col("user_id")).having(col("count")).having(col("total"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders GROUP BY user_id HAVING count AND total"
            assert result.params == {}

        def it_accepts_multiple_conditions_in_one_call() -> None:
            """having() should accept multiple conditions."""
            q = ref("orders").select(col("user_id")).group_by(col("user_id")).having(col("count"), col("total"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders GROUP BY user_id HAVING count AND total"
            assert result.params == {}

        def it_works_with_where_and_group_by() -> None:
            """having() should work with WHERE and GROUP BY."""
            q = ref("orders").select(col("user_id")).where(col("status")).group_by(col("user_id")).having(col("count"))
            result = render(q)
            assert result.query == "SELECT user_id FROM orders WHERE status GROUP BY user_id HAVING count"
            assert result.params == {}

    def describe_order_by() -> None:
        def it_renders_order_by_clause() -> None:
            """ORDER BY clause should render correctly."""
            q = ref("users").select(col("id"), col("name")).order_by(col("name"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users ORDER BY name"
            assert result.params == {}

        def it_accepts_multiple_columns() -> None:
            """order_by() should accept multiple columns."""
            q = ref("users").select(col("id"), col("name")).order_by(col("name"), col("id"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users ORDER BY name, id"
            assert result.params == {}

        def it_replaces_on_multiple_calls() -> None:
            """Second order_by() call should replace first."""
            q = ref("users").select(col("id"), col("name")).order_by(col("name")).order_by(col("id"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users ORDER BY id"
            assert result.params == {}

        def it_works_before_select() -> None:
            """order_by() can be called before select()."""
            q = ref("users").order_by(col("name")).select(col("id"), col("name"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users ORDER BY name"
            assert result.params == {}

        def it_works_without_select() -> None:
            """order_by() transforms Reference even without select()."""
            q = ref("users").order_by(col("name"))
            result = render(q)
            assert result.query == "FROM users ORDER BY name"
            assert result.params == {}

        def it_preserves_ref_alias() -> None:
            """order_by() should preserve reference alias."""
            q = ref("users").alias("u").select(col("u.id"), col("u.name")).order_by(col("u.name"))
            result = render(q)
            assert result.query == "SELECT u.id, u.name FROM users AS u ORDER BY u.name"
            assert result.params == {}

        def it_chains_with_where() -> None:
            """order_by() should chain with WHERE clause."""
            q = ref("users").select(col("id"), col("name")).where(col("active")).order_by(col("name"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users WHERE active ORDER BY name"
            assert result.params == {}

    def describe_limit() -> None:
        def it_renders_limit_clause() -> None:
            """LIMIT clause should render correctly."""
            q = ref("users").select(col("id"), col("name")).limit(10)
            result = render(q)
            assert result.query == "SELECT id, name FROM users LIMIT 10"
            assert result.params == {}

        def it_renders_limit_with_offset() -> None:
            """LIMIT with OFFSET should render correctly."""
            q = ref("users").select(col("id"), col("name")).offset(20).limit(10)
            result = render(q)
            assert result.query == "SELECT id, name FROM users LIMIT 10 OFFSET 20"
            assert result.params == {}

        def it_replaces_on_multiple_calls() -> None:
            """Second limit() call should replace first."""
            q = ref("users").select(col("id"), col("name")).limit(10).offset(2).limit(5)
            result = render(q)
            assert result.query == "SELECT id, name FROM users LIMIT 5 OFFSET 2"
            assert result.params == {}

        def it_works_before_select() -> None:
            """limit() can be called before select()."""
            q = ref("users").limit(10).select(col("id"), col("name"))
            result = render(q)
            assert result.query == "SELECT id, name FROM users LIMIT 10"
            assert result.params == {}

        def it_works_without_select() -> None:
            """limit() transforms Reference even without select()."""
            q = ref("users").limit(10)
            result = render(q)
            assert result.query == "FROM users LIMIT 10"
            assert result.params == {}

        def it_preserves_ref_alias() -> None:
            """limit() should preserve reference alias."""
            q = ref("users").alias("u").select(col("u.id")).limit(10)
            result = render(q)
            assert result.query == "SELECT u.id FROM users AS u LIMIT 10"
            assert result.params == {}

        def it_chains_with_where_and_order_by() -> None:
            """limit() should chain with WHERE and ORDER BY."""
            q = (
                ref("users")
                .select(col("id"), col("name"))
                .where(col("active"))
                .order_by(col("name"))
                .offset(5)
                .limit(10)
            )
            result = render(q)
            assert result.query == "SELECT id, name FROM users WHERE active ORDER BY name LIMIT 10 OFFSET 5"
            assert result.params == {}

    def describe_distinct() -> None:
        def it_renders_distinct_clause() -> None:
            """DISTINCT clause should render correctly."""
            q = ref("users").select(col("name")).distinct()
            result = render(q)
            assert result.query == "SELECT DISTINCT name FROM users"
            assert result.params == {}

        def it_renders_distinct_with_multiple_columns() -> None:
            """DISTINCT with multiple columns should render correctly."""
            q = ref("users").select(col("name"), col("email")).distinct()
            result = render(q)
            assert result.query == "SELECT DISTINCT name, email FROM users"
            assert result.params == {}

        def it_works_before_select() -> None:
            """distinct() can be called before select()."""
            q = ref("users").distinct().select(col("name"))
            result = render(q)
            assert result.query == "SELECT DISTINCT name FROM users"
            assert result.params == {}

        def it_works_without_select() -> None:
            """distinct() transforms Reference even without select()."""
            q = ref("users").distinct()
            result = render(q)
            assert result.query == "FROM users"
            assert result.params == {}

        def it_preserves_ref_alias() -> None:
            """distinct() should preserve reference alias."""
            q = ref("users").alias("u").select(col("u.name")).distinct()
            result = render(q)
            assert result.query == "SELECT DISTINCT u.name FROM users AS u"
            assert result.params == {}

        def it_chains_with_where() -> None:
            """distinct() should chain with WHERE clause."""
            q = ref("users").select(col("name")).where(col("active")).distinct()
            result = render(q)
            assert result.query == "SELECT DISTINCT name FROM users WHERE active"
            assert result.params == {}

        def it_chains_with_order_by_and_limit() -> None:
            """distinct() should chain with ORDER BY and LIMIT."""
            q = ref("users").select(col("name")).distinct().where(col("active")).order_by(col("name")).limit(10)
            result = render(q)
            assert result.query == "SELECT DISTINCT name FROM users WHERE active ORDER BY name LIMIT 10"
            assert result.params == {}


def describe_offset() -> None:
    def it_renders_offset_alone() -> None:
        """offset() alone should render OFFSET clause."""
        q = ref("users").select(col("*")).offset(10)
        result = render(q)
        assert result.query == "SELECT * FROM users OFFSET 10"

    def it_renders_with_limit() -> None:
        """offset() with limit() should render LIMIT OFFSET."""
        q = ref("users").select(col("*")).offset(20).limit(10)
        result = render(q)
        assert result.query == "SELECT * FROM users LIMIT 10 OFFSET 20"

    def it_can_be_called_after_limit() -> None:
        """offset() and limit() can be called in either order."""
        q = ref("users").select(col("*")).limit(10).offset(20)
        result = render(q)
        assert result.query == "SELECT * FROM users LIMIT 10 OFFSET 20"

    def it_works_with_order_by() -> None:
        """offset() should work with ORDER BY for pagination."""
        q = ref("posts").select(col("*")).order_by(col("created_at").desc()).offset(50).limit(25)
        result = render(q)
        assert result.query == "SELECT * FROM posts ORDER BY created_at DESC LIMIT 25 OFFSET 50"

    def it_replaces_previous_offset() -> None:
        """Multiple offset() calls - last wins."""
        q = ref("users").select(col("*")).offset(10).offset(20)
        result = render(q)
        assert result.query == "SELECT * FROM users OFFSET 20"
