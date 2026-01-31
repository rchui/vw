"""Tests for complex method chaining."""

from vw.postgres import col, render, source


def describe_method_chaining() -> None:
    def it_chains_all_clauses_in_order() -> None:
        """All clauses should chain correctly in SQL order."""
        q = (
            source("orders")
            .alias("o")
            .select(col("o.user_id"), col("o.total"))
            .where(col("o.status"))
            .group_by(col("o.user_id"))
            .having(col("count"))
            .order_by(col("o.total"))
            .limit(10, offset=5)
        )
        expected = (
            "SELECT o.user_id, o.total FROM orders AS o "
            "WHERE o.status GROUP BY o.user_id HAVING count "
            "ORDER BY o.total LIMIT 10 OFFSET 5"
        )
        assert render(q) == expected

    def it_chains_select_where_order_limit() -> None:
        """Common query pattern should chain correctly."""
        q = (
            source("users")
            .select(col("id"), col("name"), col("email"))
            .where(col("active"), col("verified"))
            .order_by(col("name"))
            .limit(20)
        )
        expected = "SELECT id, name, email FROM users WHERE active AND verified ORDER BY name LIMIT 20"
        assert render(q) == expected

    def it_chains_distinct_with_where_order() -> None:
        """DISTINCT should chain with WHERE and ORDER BY."""
        q = source("products").select(col("category")).distinct().where(col("in_stock")).order_by(col("category"))
        expected = "SELECT DISTINCT category FROM products WHERE in_stock ORDER BY category"
        assert render(q) == expected

    def it_chains_group_by_with_having_order_limit() -> None:
        """GROUP BY with HAVING, ORDER BY, and LIMIT should chain."""
        q = (
            source("sales")
            .select(col("product_id"), col("total"))
            .group_by(col("product_id"))
            .having(col("total"), col("count"))
            .order_by(col("total"))
            .limit(5)
        )
        expected = (
            "SELECT product_id, total FROM sales GROUP BY product_id HAVING total AND count ORDER BY total LIMIT 5"
        )
        assert render(q) == expected

    def it_allows_reordering_method_calls() -> None:
        """Methods can be called in any order (transforms Source once)."""
        q = source("users").where(col("active")).limit(10).select(col("id")).order_by(col("id"))
        expected = "SELECT id FROM users WHERE active ORDER BY id LIMIT 10"
        assert render(q) == expected

    def it_uses_rowset_col_for_qualified_columns() -> None:
        """Using RowSet.col() should create qualified columns."""
        s = source("users").alias("u")
        q = s.select(s.col("id"), s.col("name")).where(s.col("active")).order_by(s.col("name"))
        expected = "SELECT u.id, u.name FROM users AS u WHERE u.active ORDER BY u.name"
        assert render(q) == expected

    def it_uses_rowset_star_in_select() -> None:
        """Using RowSet.star should select all columns."""
        s = source("users").alias("u")
        q = s.select(s.star).where(s.col("active")).limit(10)
        expected = "SELECT u.* FROM users AS u WHERE u.active LIMIT 10"
        assert render(q) == expected

    def it_builds_aggregation_query() -> None:
        """Complex aggregation query should build correctly."""
        s = source("orders").alias("o")
        q = (
            s.select(s.col("user_id"), s.col("total"))
            .where(s.col("created_at"), s.col("status"))
            .group_by(s.col("user_id"))
            .having(s.col("count"))
            .order_by(s.col("total"))
            .limit(100)
        )
        expected = (
            "SELECT o.user_id, o.total FROM orders AS o "
            "WHERE o.created_at AND o.status "
            "GROUP BY o.user_id HAVING o.count "
            "ORDER BY o.total LIMIT 100"
        )
        assert render(q) == expected

    def it_allows_multiple_where_calls() -> None:
        """Multiple where() calls should accumulate."""
        q = source("users").select(col("id")).where(col("active")).where(col("verified")).where(col("premium"))
        expected = "SELECT id FROM users WHERE active AND verified AND premium"
        assert render(q) == expected

    def it_replaces_on_repeated_select() -> None:
        """Second select() should replace columns."""
        q = source("users").select(col("id"), col("name")).where(col("active")).select(col("email"))
        expected = "SELECT email FROM users WHERE active"
        assert render(q) == expected
