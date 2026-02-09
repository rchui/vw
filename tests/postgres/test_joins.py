"""Unit tests for join state construction."""

import pytest

from vw.core.states import Column, Join, Statement
from vw.postgres import col, ref


def describe_join_dataclass():
    def it_creates_join_with_on_clause():
        """Join should be created with ON clause."""
        orders = ref("orders")

        join = Join(
            jtype="INNER",
            right=orders.state,
            on=(Column(name="id"), Column(name="user_id")),
        )

        assert join.jtype == "INNER"
        assert join.right == orders.state
        assert len(join.on) == 2
        assert len(join.using) == 0

    def it_creates_join_with_using_clause():
        """Join should be created with USING clause."""
        orders = ref("orders")

        join = Join(
            jtype="LEFT",
            right=orders.state,
            using=(Column(name="user_id"),),
        )

        assert join.jtype == "LEFT"
        assert join.right == orders.state
        assert len(join.on) == 0
        assert len(join.using) == 1

    def it_is_frozen():
        """Join should be immutable."""
        orders = ref("orders")

        join = Join(
            jtype="INNER",
            right=orders.state,
            on=(Column(name="id"),),
        )

        with pytest.raises(AttributeError):
            join.jtype = "LEFT"  # type: ignore


def describe_join_accessor():
    def it_creates_inner_join_state():
        """JoinAccessor.inner() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.inner(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "INNER"

    def it_creates_left_join_state():
        """JoinAccessor.left() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.left(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "LEFT"

    def it_creates_right_join_state():
        """JoinAccessor.right() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.right(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "RIGHT"

    def it_creates_full_outer_join_state():
        """JoinAccessor.full_outer() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.full_outer(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "FULL"

    def it_creates_cross_join_state():
        """JoinAccessor.cross() should create correct state."""
        users = ref("users")
        tags = ref("tags")

        query = users.join.cross(tags)

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "CROSS"
        assert len(query.state.joins[0].on) == 0
        assert len(query.state.joins[0].using) == 0

    def it_accumulates_multiple_joins():
        """Multiple join calls should accumulate."""
        users = ref("users")
        orders = ref("orders")
        products = ref("products")

        query = users.join.inner(orders, on=[col("id") == col("user_id")]).join.left(
            products, on=[col("product_id") == col("id")]
        )

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 2
        assert query.state.joins[0].jtype == "INNER"
        assert query.state.joins[1].jtype == "LEFT"


def describe_statement_joins_field():
    def it_defaults_to_empty_tuple():
        """Statement.joins should default to empty tuple."""
        users = ref("users")
        query = users.select(col("id"))

        assert isinstance(query.state, Statement)
        assert query.state.joins == ()
        assert len(query.state.joins) == 0

    def it_preserves_joins_with_other_operations():
        """Joins should be preserved when adding other clauses."""
        users = ref("users")
        orders = ref("orders")

        query = (
            users.join.inner(orders, on=[col("id") == col("user_id")])
            .select(col("id"))
            .where(col("active") == col("true"))
            .order_by(col("id"))
        )

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == "INNER"


def describe_join_with_using_clause():
    def it_creates_join_with_using():
        """Join should support USING clause."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.inner(orders, using=[col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert len(query.state.joins[0].using) == 1
        assert len(query.state.joins[0].on) == 0

    def it_creates_join_with_both_on_and_using():
        """Join should allow both ON and USING (no validation)."""
        users = ref("users")
        orders = ref("orders")

        # This is allowed (no validation), but will error in PostgreSQL
        query = users.join.inner(
            orders,
            on=[col("id") == col("user_id")],
            using=[col("user_id")],
        )

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert len(query.state.joins[0].on) == 1
        assert len(query.state.joins[0].using) == 1


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
