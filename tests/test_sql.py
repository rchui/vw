"""Integration tests for SQL generation."""

import vw


def describe_basic_select():
    """Tests for basic SELECT statements."""

    def it_generates_select_star():
        """Should generate SELECT * FROM table."""
        sql = vw.Source("users").select(vw.col("*")).render()
        assert sql == "SELECT * FROM users"

    def it_generates_select_single_column():
        """Should generate SELECT column FROM table."""
        sql = vw.Source("products").select(vw.col("id")).render()
        assert sql == "SELECT id FROM products"

    def it_generates_select_multiple_columns():
        """Should generate SELECT col1, col2, col3 FROM table."""
        sql = vw.Source("orders").select(vw.col("id"), vw.col("customer_id"), vw.col("total")).render()
        assert sql == "SELECT id, customer_id, total FROM orders"


def describe_star_extensions():
    """Tests for star expression extensions."""

    def it_generates_star_replace():
        """Should generate SELECT * REPLACE (...) FROM table."""
        sql = vw.Source("users").select(vw.col("* REPLACE (name AS full_name)")).render()
        assert sql == "SELECT * REPLACE (name AS full_name) FROM users"

    def it_generates_star_exclude():
        """Should generate SELECT * EXCLUDE (...) FROM table."""
        sql = vw.Source("users").select(vw.col("* EXCLUDE (password)")).render()
        assert sql == "SELECT * EXCLUDE (password) FROM users"

    def it_generates_star_exclude_multiple():
        """Should generate SELECT * EXCLUDE with multiple columns."""
        sql = vw.Source("users").select(vw.col("* EXCLUDE (password, ssn)")).render()
        assert sql == "SELECT * EXCLUDE (password, ssn) FROM users"


def describe_method_chaining():
    """Tests for method chaining patterns."""

    def it_chains_source_select_render():
        """Should support fluent method chaining."""
        sql = vw.Source("employees").select(vw.col("id"), vw.col("name")).render()
        assert sql == "SELECT id, name FROM employees"

    def it_allows_breaking_chain_into_steps():
        """Should allow storing intermediate objects."""
        source = vw.Source("products")
        statement = source.select(vw.col("*"))
        sql = statement.render()
        assert sql == "SELECT * FROM products"


def describe_complex_expressions():
    """Tests for complex SQL expressions via escape hatch."""

    def it_generates_cast_expression():
        """Should support CAST expressions."""
        sql = vw.Source("sales").select(vw.col("CAST(price AS DECIMAL(10,2))")).render()
        assert sql == "SELECT CAST(price AS DECIMAL(10,2)) FROM sales"

    def it_mixes_simple_and_complex_columns():
        """Should support mixing simple columns and complex expressions."""
        sql = (
            vw.Source("orders")
            .select(vw.col("id"), vw.col("ROUND(total, 2) AS rounded_total"), vw.col("status"))
            .render()
        )
        assert sql == "SELECT id, ROUND(total, 2) AS rounded_total, status FROM orders"
