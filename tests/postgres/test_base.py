"""Tests for RowSet methods."""

from vw.postgres import col, render, source


def describe_rowset() -> None:
    def describe_col() -> None:
        def it_creates_unqualified_column_without_alias() -> None:
            """col() should create unqualified column when source has no alias."""
            s = source("users")
            c = s.col("id")
            assert render(c) == "id"

        def it_creates_qualified_column_with_alias() -> None:
            """col() should create qualified column when source has alias."""
            s = source("users").alias("u")
            c = s.col("id")
            assert render(c) == "u.id"

        def it_works_in_select() -> None:
            """RowSet.col() should work in select()."""
            s = source("users").alias("u")
            q = s.select(s.col("id"), s.col("name"))
            assert render(q) == "SELECT u.id, u.name FROM users AS u"

        def it_works_in_where() -> None:
            """RowSet.col() should work in where()."""
            s = source("users").alias("u")
            q = s.select(s.col("id")).where(s.col("active"))
            assert render(q) == "SELECT u.id FROM users AS u WHERE u.active"

        def it_works_with_statement_alias() -> None:
            """col() should use Statement alias when available."""
            s = source("users").select(col("id")).alias("subq")
            c = s.col("id")
            assert render(c) == "subq.id"

        def it_creates_column_expression() -> None:
            """col() should create Expression with Column state."""
            from vw.core.states import Column

            s = source("users").alias("u")
            c = s.col("id")
            assert isinstance(c.state, Column)
            assert c.state.name == "u.id"

    def describe_star() -> None:
        def it_creates_unqualified_star_without_alias() -> None:
            """star should create unqualified star when source has no alias."""
            s = source("users")
            assert render(s.star) == "*"

        def it_creates_qualified_star_with_alias() -> None:
            """star should create qualified star when source has alias."""
            s = source("users").alias("u")
            assert render(s.star) == "u.*"

        def it_works_in_select() -> None:
            """RowSet.star should work in select()."""
            s = source("users").alias("u")
            q = s.select(s.star)
            assert render(q) == "SELECT u.* FROM users AS u"

        def it_works_without_alias_in_select() -> None:
            """RowSet.star should work in select without alias."""
            s = source("users")
            q = s.select(s.star)
            assert render(q) == "SELECT * FROM users"

        def it_works_with_statement_alias() -> None:
            """star should use Statement alias when available."""
            s = source("users").select(col("id")).alias("subq")
            assert render(s.star) == "subq.*"

        def it_creates_column_expression_with_star() -> None:
            """star should create Expression with Column state using *."""
            from vw.core.states import Column

            s = source("users").alias("u")
            star = s.star
            assert isinstance(star.state, Column)
            assert star.state.name == "u.*"

        def it_is_a_property_not_method() -> None:
            """star should be a property, not a method."""
            s = source("users")
            star = s.star
            assert render(star) == "*"

    def describe_where() -> None:
        def it_transforms_source_to_statement() -> None:
            """where() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("users")
            assert isinstance(s.state, Source)

            q = s.where(col("active"))
            assert isinstance(q.state, Statement)

        def it_renders_where_clause() -> None:
            """WHERE clause should render correctly."""
            q = source("users").select(col("id")).where(col("active"))
            assert render(q) == "SELECT id FROM users WHERE active"

        def it_accumulates_multiple_conditions() -> None:
            """Multiple where() calls should accumulate conditions."""
            q = source("users").select(col("id")).where(col("active")).where(col("verified"))
            assert render(q) == "SELECT id FROM users WHERE active AND verified"

        def it_accepts_multiple_conditions_in_one_call() -> None:
            """where() should accept multiple conditions."""
            q = source("users").select(col("id")).where(col("active"), col("verified"))
            assert render(q) == "SELECT id FROM users WHERE active AND verified"

        def it_works_before_select() -> None:
            """where() can be called before select()."""
            q = source("users").where(col("active")).select(col("id"))
            assert render(q) == "SELECT id FROM users WHERE active"

        def it_works_without_select() -> None:
            """where() transforms Source even without select()."""
            q = source("users").where(col("active"))
            assert render(q) == "FROM users WHERE active"

        def it_preserves_source_alias() -> None:
            """where() should preserve source alias."""
            q = source("users").alias("u").select(col("id")).where(col("u.active"))
            assert render(q) == "SELECT id FROM users AS u WHERE u.active"

        def it_chains_with_multiple_clauses() -> None:
            """where() should chain with other clauses."""
            q = source("users").select(col("id"), col("name")).where(col("active")).limit(10)
            assert render(q) == "SELECT id, name FROM users WHERE active LIMIT 10"

    def describe_group_by() -> None:
        def it_transforms_source_to_statement() -> None:
            """group_by() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("orders")
            assert isinstance(s.state, Source)

            q = s.group_by(col("user_id"))
            assert isinstance(q.state, Statement)

        def it_renders_group_by_clause() -> None:
            """GROUP BY clause should render correctly."""
            q = source("orders").select(col("user_id")).group_by(col("user_id"))
            assert render(q) == "SELECT user_id FROM orders GROUP BY user_id"

        def it_accepts_multiple_columns() -> None:
            """group_by() should accept multiple columns."""
            q = source("orders").select(col("user_id"), col("product_id")).group_by(col("user_id"), col("product_id"))
            assert render(q) == "SELECT user_id, product_id FROM orders GROUP BY user_id, product_id"

        def it_replaces_on_multiple_calls() -> None:
            """Second group_by() call should replace first."""
            q = source("orders").select(col("product_id")).group_by(col("user_id")).group_by(col("product_id"))
            assert render(q) == "SELECT product_id FROM orders GROUP BY product_id"

        def it_works_before_select() -> None:
            """group_by() can be called before select()."""
            q = source("orders").group_by(col("user_id")).select(col("user_id"))
            assert render(q) == "SELECT user_id FROM orders GROUP BY user_id"

        def it_preserves_source_alias() -> None:
            """group_by() should preserve source alias."""
            q = source("orders").alias("o").select(col("o.user_id")).group_by(col("o.user_id"))
            assert render(q) == "SELECT o.user_id FROM orders AS o GROUP BY o.user_id"

    def describe_having() -> None:
        def it_transforms_source_to_statement() -> None:
            """having() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("orders")
            assert isinstance(s.state, Source)

            q = s.having(col("count"))
            assert isinstance(q.state, Statement)

        def it_renders_having_clause() -> None:
            """HAVING clause should render correctly."""
            q = source("orders").select(col("user_id")).group_by(col("user_id")).having(col("count"))
            assert render(q) == "SELECT user_id FROM orders GROUP BY user_id HAVING count"

        def it_accumulates_multiple_conditions() -> None:
            """Multiple having() calls should accumulate conditions."""
            q = (
                source("orders")
                .select(col("user_id"))
                .group_by(col("user_id"))
                .having(col("count"))
                .having(col("total"))
            )
            assert render(q) == "SELECT user_id FROM orders GROUP BY user_id HAVING count AND total"

        def it_accepts_multiple_conditions_in_one_call() -> None:
            """having() should accept multiple conditions."""
            q = source("orders").select(col("user_id")).group_by(col("user_id")).having(col("count"), col("total"))
            assert render(q) == "SELECT user_id FROM orders GROUP BY user_id HAVING count AND total"

        def it_works_with_where_and_group_by() -> None:
            """having() should work with WHERE and GROUP BY."""
            q = (
                source("orders")
                .select(col("user_id"))
                .where(col("status"))
                .group_by(col("user_id"))
                .having(col("count"))
            )
            assert render(q) == "SELECT user_id FROM orders WHERE status GROUP BY user_id HAVING count"

    def describe_order_by() -> None:
        def it_transforms_source_to_statement() -> None:
            """order_by() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("users")
            assert isinstance(s.state, Source)

            q = s.order_by(col("name"))
            assert isinstance(q.state, Statement)

        def it_renders_order_by_clause() -> None:
            """ORDER BY clause should render correctly."""
            q = source("users").select(col("id"), col("name")).order_by(col("name"))
            assert render(q) == "SELECT id, name FROM users ORDER BY name"

        def it_accepts_multiple_columns() -> None:
            """order_by() should accept multiple columns."""
            q = source("users").select(col("id"), col("name")).order_by(col("name"), col("id"))
            assert render(q) == "SELECT id, name FROM users ORDER BY name, id"

        def it_replaces_on_multiple_calls() -> None:
            """Second order_by() call should replace first."""
            q = source("users").select(col("id"), col("name")).order_by(col("name")).order_by(col("id"))
            assert render(q) == "SELECT id, name FROM users ORDER BY id"

        def it_works_before_select() -> None:
            """order_by() can be called before select()."""
            q = source("users").order_by(col("name")).select(col("id"), col("name"))
            assert render(q) == "SELECT id, name FROM users ORDER BY name"

        def it_works_without_select() -> None:
            """order_by() transforms Source even without select()."""
            q = source("users").order_by(col("name"))
            assert render(q) == "FROM users ORDER BY name"

        def it_preserves_source_alias() -> None:
            """order_by() should preserve source alias."""
            q = source("users").alias("u").select(col("u.id"), col("u.name")).order_by(col("u.name"))
            assert render(q) == "SELECT u.id, u.name FROM users AS u ORDER BY u.name"

        def it_chains_with_where() -> None:
            """order_by() should chain with WHERE clause."""
            q = source("users").select(col("id"), col("name")).where(col("active")).order_by(col("name"))
            assert render(q) == "SELECT id, name FROM users WHERE active ORDER BY name"

    def describe_limit() -> None:
        def it_transforms_source_to_statement() -> None:
            """limit() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("users")
            assert isinstance(s.state, Source)

            q = s.limit(10)
            assert isinstance(q.state, Statement)

        def it_renders_limit_clause() -> None:
            """LIMIT clause should render correctly."""
            q = source("users").select(col("id"), col("name")).limit(10)
            assert render(q) == "SELECT id, name FROM users LIMIT 10"

        def it_renders_limit_with_offset() -> None:
            """LIMIT with OFFSET should render correctly."""
            q = source("users").select(col("id"), col("name")).limit(10, offset=20)
            assert render(q) == "SELECT id, name FROM users LIMIT 10 OFFSET 20"

        def it_replaces_on_multiple_calls() -> None:
            """Second limit() call should replace first."""
            q = source("users").select(col("id"), col("name")).limit(10).limit(5, offset=2)
            assert render(q) == "SELECT id, name FROM users LIMIT 5 OFFSET 2"

        def it_works_before_select() -> None:
            """limit() can be called before select()."""
            q = source("users").limit(10).select(col("id"), col("name"))
            assert render(q) == "SELECT id, name FROM users LIMIT 10"

        def it_works_without_select() -> None:
            """limit() transforms Source even without select()."""
            q = source("users").limit(10)
            assert render(q) == "FROM users LIMIT 10"

        def it_preserves_source_alias() -> None:
            """limit() should preserve source alias."""
            q = source("users").alias("u").select(col("u.id")).limit(10)
            assert render(q) == "SELECT u.id FROM users AS u LIMIT 10"

        def it_chains_with_where_and_order_by() -> None:
            """limit() should chain with WHERE and ORDER BY."""
            q = (
                source("users")
                .select(col("id"), col("name"))
                .where(col("active"))
                .order_by(col("name"))
                .limit(10, offset=5)
            )
            assert render(q) == "SELECT id, name FROM users WHERE active ORDER BY name LIMIT 10 OFFSET 5"

        def it_creates_limit_with_correct_fields() -> None:
            """limit() should create Limit dataclass with correct fields."""
            from vw.core.states import Limit

            q = source("users").limit(10, offset=20)
            assert isinstance(q.state.limit, Limit)
            assert q.state.limit.count == 10
            assert q.state.limit.offset == 20

    def describe_distinct() -> None:
        def it_transforms_source_to_statement() -> None:
            """distinct() should transform Source to Statement."""
            from vw.core.states import Source, Statement

            s = source("users")
            assert isinstance(s.state, Source)

            q = s.distinct()
            assert isinstance(q.state, Statement)

        def it_renders_distinct_clause() -> None:
            """DISTINCT clause should render correctly."""
            q = source("users").select(col("name")).distinct()
            assert render(q) == "SELECT DISTINCT name FROM users"

        def it_renders_distinct_with_multiple_columns() -> None:
            """DISTINCT with multiple columns should render correctly."""
            q = source("users").select(col("name"), col("email")).distinct()
            assert render(q) == "SELECT DISTINCT name, email FROM users"

        def it_works_before_select() -> None:
            """distinct() can be called before select()."""
            q = source("users").distinct().select(col("name"))
            assert render(q) == "SELECT DISTINCT name FROM users"

        def it_works_without_select() -> None:
            """distinct() transforms Source even without select()."""
            q = source("users").distinct()
            assert render(q) == "FROM users"

        def it_preserves_source_alias() -> None:
            """distinct() should preserve source alias."""
            q = source("users").alias("u").select(col("u.name")).distinct()
            assert render(q) == "SELECT DISTINCT u.name FROM users AS u"

        def it_chains_with_where() -> None:
            """distinct() should chain with WHERE clause."""
            q = source("users").select(col("name")).where(col("active")).distinct()
            assert render(q) == "SELECT DISTINCT name FROM users WHERE active"

        def it_chains_with_order_by_and_limit() -> None:
            """distinct() should chain with ORDER BY and LIMIT."""
            q = source("users").select(col("name")).distinct().where(col("active")).order_by(col("name")).limit(10)
            assert render(q) == "SELECT DISTINCT name FROM users WHERE active ORDER BY name LIMIT 10"

        def it_creates_distinct_instance() -> None:
            """distinct() should create Distinct dataclass instance."""
            from vw.core.states import Distinct

            q = source("users").distinct()
            assert isinstance(q.state.distinct, Distinct)
