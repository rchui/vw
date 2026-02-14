"""Unit tests for subquery state construction."""

from vw.core.states import Exists, Statement
from vw.postgres import col, exists, param, ref


def describe_exists_state():
    def it_creates_exists_state():
        """Test EXISTS state construction."""
        orders = ref("orders")
        subquery = orders.where(col("status") == param("status", "active"))

        expr = exists(subquery)

        assert isinstance(expr.state, Exists)
        assert isinstance(expr.state.subquery, Statement)

    def it_wraps_exists_in_expression():
        """Test exists() returns Expression."""
        orders = ref("orders")
        subquery = orders.where(col("status") == param("status", "active"))

        expr = exists(subquery)

        # Should be an Expression
        assert hasattr(expr, "state")
        assert isinstance(expr.state, Exists)


def describe_exists_with_complex_subquery():
    def it_creates_exists_with_joins():
        """Test EXISTS with complex subquery including joins."""
        users = ref("users").alias("u")
        orders = ref("orders").alias("o")
        products = ref("products").alias("p")

        subquery = (
            orders.join.inner(products, on=[orders.col("product_id") == products.col("id")])
            .where(orders.col("user_id") == users.col("id"))
            .where(products.col("category") == param("category", "electronics"))
        )

        expr = exists(subquery)

        assert isinstance(expr.state, Exists)
        assert isinstance(expr.state.subquery, Statement)
        # Verify joins are present
        assert len(expr.state.subquery.joins) == 1
