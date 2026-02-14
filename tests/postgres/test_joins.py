"""Unit tests for PostgreSQL-specific join features.

This test suite covers PostgreSQL-specific join functionality:
- LATERAL joins (inner, left, right, full outer, cross)
- Lateral join state construction and validation

Standard ANSI SQL join tests are in tests/core/test_joins.py.
"""

from vw.core.states import Join, Statement
from vw.postgres import col, ref


def describe_lateral_joins():
    def it_creates_join_with_lateral_true():
        """Join dataclass should support lateral=True."""
        orders = ref("orders")

        join = Join(jtype="INNER", right=orders.state, on=(), lateral=True)

        assert join.lateral is True

    def it_defaults_lateral_to_false():
        """Join dataclass should default lateral to False."""
        orders = ref("orders")

        join = Join(jtype="INNER", right=orders.state, on=())

        assert join.lateral is False

    def it_creates_inner_join_with_lateral():
        """JoinAccessor.inner() should accept lateral parameter."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.inner(orders, on=[col("id") == col("user_id")], lateral=True)

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].lateral is True

    def it_creates_left_join_with_lateral():
        """JoinAccessor.left() should accept lateral parameter."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.left(orders, on=[col("id") == col("user_id")], lateral=True)

        assert isinstance(query.state, Statement)
        assert query.state.joins[0].lateral is True

    def it_creates_right_join_with_lateral():
        """JoinAccessor.right() should accept lateral parameter."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.right(orders, on=[col("id") == col("user_id")], lateral=True)

        assert isinstance(query.state, Statement)
        assert query.state.joins[0].lateral is True

    def it_creates_full_outer_join_with_lateral():
        """JoinAccessor.full_outer() should accept lateral parameter."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.full_outer(orders, on=[col("id") == col("user_id")], lateral=True)

        assert isinstance(query.state, Statement)
        assert query.state.joins[0].lateral is True

    def it_creates_cross_join_with_lateral():
        """JoinAccessor.cross() should accept lateral parameter."""
        users = ref("users")
        series = ref("generate_series(1, 5)")

        query = users.join.cross(series, lateral=True)

        assert isinstance(query.state, Statement)
        assert query.state.joins[0].lateral is True

    def it_defaults_lateral_to_false_in_join_methods():
        """Join methods should default lateral to False when not specified."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.inner(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert query.state.joins[0].lateral is False
