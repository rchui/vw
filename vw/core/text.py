"""String function accessor for Expression (.text property)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT

if TYPE_CHECKING:
    from vw.core.base import Expression


class TextAccessor(Generic[ExprT, RowSetT]):
    """String function accessor for Expression (.text property).

    Provides SQL string functions as methods on a text-typed expression.
    Access via: col("name").text.upper()
    """

    def __init__(self, expr: Expression[ExprT, RowSetT]) -> None:
        self.expr = expr

    def upper(self) -> ExprT:
        """UPPER(expr) — convert string to uppercase."""
        from vw.core.states import Function

        state = Function(name="UPPER", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def lower(self) -> ExprT:
        """LOWER(expr) — convert string to lowercase."""
        from vw.core.states import Function

        state = Function(name="LOWER", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def trim(self) -> ExprT:
        """TRIM(expr) — remove leading and trailing whitespace."""
        from vw.core.states import Function

        state = Function(name="TRIM", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def ltrim(self) -> ExprT:
        """LTRIM(expr) — remove leading whitespace."""
        from vw.core.states import Function

        state = Function(name="LTRIM", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def rtrim(self) -> ExprT:
        """RTRIM(expr) — remove trailing whitespace."""
        from vw.core.states import Function

        state = Function(name="RTRIM", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def length(self) -> ExprT:
        """LENGTH(expr) — number of characters in string."""
        from vw.core.states import Function

        state = Function(name="LENGTH", args=(self.expr.state,))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def substring(self, start: int, length: int | None = None) -> ExprT:
        """SUBSTRING(expr, start[, length]) — extract substring.

        Args:
            start: 1-based start position.
            length: Number of characters to extract (optional).
        """
        from vw.core.states import Function, Literal

        if length is None:
            state = Function(name="SUBSTRING", args=(self.expr.state, Literal(value=start)))
        else:
            state = Function(name="SUBSTRING", args=(self.expr.state, Literal(value=start), Literal(value=length)))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def replace(self, old: ExprT, new: ExprT) -> ExprT:
        """REPLACE(expr, old, new) — replace occurrences of old with new.

        Args:
            old: The substring to find.
            new: The replacement substring.
        """
        from vw.core.states import Function

        state = Function(name="REPLACE", args=(self.expr.state, old.state, new.state))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)

    def concat(self, *others: ExprT) -> ExprT:
        """CONCAT(expr, ...) — concatenate strings.

        Args:
            *others: One or more expressions to concatenate.
        """
        from vw.core.states import Function

        state = Function(name="CONCAT", args=(self.expr.state, *(o.state for o in others)))
        return self.expr.factories.expr(state=state, factories=self.expr.factories)
