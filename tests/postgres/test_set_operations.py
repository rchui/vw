"""Unit tests for set operation state construction."""

from vw.core.states import SetOperationState, Statement
from vw.postgres import col, ref


def describe_set_operation_state():
    def it_creates_union_state():
        """Test UNION state construction."""
        query1 = ref("users").select(col("id"))
        query2 = ref("admins").select(col("id"))

        setop = query1 | query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "UNION"
        assert isinstance(setop.state.left, Statement)
        assert isinstance(setop.state.right, Statement)

    def it_creates_union_all_state():
        """Test UNION ALL state construction."""
        query1 = ref("users").select(col("id"))
        query2 = ref("admins").select(col("id"))

        setop = query1 + query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "UNION ALL"

    def it_creates_intersect_state():
        """Test INTERSECT state construction."""
        query1 = ref("users").select(col("id"))
        query2 = ref("banned").select(col("user_id"))

        setop = query1 & query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "INTERSECT"

    def it_creates_except_state():
        """Test EXCEPT state construction."""
        query1 = ref("users").select(col("id"))
        query2 = ref("banned").select(col("user_id"))

        setop = query1 - query2

        assert isinstance(setop.state, SetOperationState)
        assert setop.state.operator == "EXCEPT"

    def it_handles_nested_set_operations():
        """Test nested set operations."""
        query1 = ref("users").select(col("id"))
        query2 = ref("admins").select(col("id"))
        query3 = ref("guests").select(col("id"))

        # (query1 UNION query2) UNION query3
        setop = (query1 | query2) | query3

        assert isinstance(setop.state, SetOperationState)
        assert isinstance(setop.state.left, SetOperationState)
        assert isinstance(setop.state.right, Statement)
