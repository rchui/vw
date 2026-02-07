"""Unit tests for join state construction."""

from vw.core.states import Join, JoinType, Statement
from vw.postgres import col, ref
from vw.postgres.base import Expression


def describe_join_type_enum():
    def it_has_correct_values():
        """JoinType enum should have the correct values."""
        assert JoinType.INNER == "INNER"
        assert JoinType.LEFT == "LEFT"
        assert JoinType.RIGHT == "RIGHT"
        assert JoinType.FULL == "FULL"
        assert JoinType.CROSS == "CROSS"

    def it_is_string_enum():
        """JoinType should be a string enum."""
        assert isinstance(JoinType.INNER, str)
        assert str(JoinType.INNER) == "INNER"


def describe_join_dataclass():
    def it_creates_join_with_on_clause():
        """Join should be created with ON clause."""
        orders = ref("orders")

        join: Join[Expression] = Join(
            jtype=JoinType.INNER,
            right=orders.state,
            on=(col("id"), col("user_id")),
        )

        assert join.jtype == JoinType.INNER
        assert join.right == orders.state
        assert len(join.on) == 2
        assert len(join.using) == 0

    def it_creates_join_with_using_clause():
        """Join should be created with USING clause."""
        orders = ref("orders")

        join: Join[Expression] = Join(
            jtype=JoinType.LEFT,
            right=orders.state,
            using=(col("user_id"),),
        )

        assert join.jtype == JoinType.LEFT
        assert join.right == orders.state
        assert len(join.on) == 0
        assert len(join.using) == 1

    def it_is_frozen():
        """Join should be immutable."""
        orders = ref("orders")

        join: Join[Expression] = Join(
            jtype=JoinType.INNER,
            right=orders.state,
            on=(col("id"),),
        )

        try:
            join.jtype = JoinType.LEFT  # type: ignore[misc]
            raise AssertionError("Should not be able to modify frozen dataclass")
        except Exception:
            pass  # Expected


def describe_join_accessor():
    def it_creates_inner_join_state():
        """JoinAccessor.inner() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.inner(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.INNER

    def it_creates_left_join_state():
        """JoinAccessor.left() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.left(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.LEFT

    def it_creates_right_join_state():
        """JoinAccessor.right() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.right(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.RIGHT

    def it_creates_full_outer_join_state():
        """JoinAccessor.full_outer() should create correct state."""
        users = ref("users")
        orders = ref("orders")

        query = users.join.full_outer(orders, on=[col("id") == col("user_id")])

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.FULL

    def it_creates_cross_join_state():
        """JoinAccessor.cross() should create correct state."""
        users = ref("users")
        tags = ref("tags")

        query = users.join.cross(tags)

        assert isinstance(query.state, Statement)
        assert len(query.state.joins) == 1
        assert query.state.joins[0].jtype == JoinType.CROSS
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
        assert query.state.joins[0].jtype == JoinType.INNER
        assert query.state.joins[1].jtype == JoinType.LEFT


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
        assert query.state.joins[0].jtype == JoinType.INNER


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
