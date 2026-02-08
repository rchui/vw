"""CASE/WHEN expression builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

from vw.core.base import ExprT, RowSetT
from vw.core.states import Expr, WhenThen

if TYPE_CHECKING:
    from vw.core.base import Factories


@dataclass(eq=False, frozen=True, kw_only=True)
class When(Generic[ExprT, RowSetT]):
    """Incomplete WHEN clause - must be completed with .then()."""

    condition: Expr
    prior_whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def then(self, result: ExprT, /) -> CaseExpression[ExprT, RowSetT]:
        """Complete this WHEN clause with a THEN result.

        Args:
            result: The result expression when the condition is true.

        Returns:
            A CaseExpression that can be extended with .when() or finalized.
        """
        new_when = WhenThen(condition=self.condition, result=result.state)
        return CaseExpression(
            whens=(*self.prior_whens, new_when),
            factories=self.factories,
        )


@dataclass(eq=False, frozen=True, kw_only=True)
class CaseExpression(Generic[ExprT, RowSetT]):
    """Accumulated CASE branches - can add more WHEN clauses or finalize."""

    whens: tuple[WhenThen, ...]
    factories: Factories[ExprT, RowSetT]

    def when(self, condition: ExprT, /) -> When[ExprT, RowSetT]:
        """Add another WHEN clause.

        Args:
            condition: The boolean condition to check.

        Returns:
            A When builder that must be completed with .then().
        """
        return When[ExprT, RowSetT](
            condition=condition.state,
            prior_whens=self.whens,
            factories=self.factories,
        )

    def otherwise(self, result: ExprT, /) -> ExprT:
        """Finalize with an ELSE clause.

        Args:
            result: The result when no WHEN conditions match.

        Returns:
            An Expression representing the complete CASE expression.
        """
        from vw.core.states import Case

        state = Case(whens=self.whens, else_result=result.state)
        return self.factories.expr(state=state, factories=self.factories)

    def end(self) -> ExprT:
        """Finalize without an ELSE clause (NULL when no branch matches).

        Returns:
            An Expression representing the complete CASE expression.
        """
        from vw.core.states import Case

        state = Case(whens=self.whens)
        return self.factories.expr(state=state, factories=self.factories)
