from __future__ import annotations

from dataclasses import dataclass

from vw.core.base import Expression as CoreExpression
from vw.core.base import RowSet as CoreRowSet


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(CoreExpression["Expression", "RowSet"]): ...


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(CoreRowSet["Expression", "RowSet"]):
    def star(self) -> Expression:
        """Create a star expression qualified with this rowset's source.

        Returns:
            An Expression with qualified star.

        Example:
            >>> users.star()  # SELECT users.*
        """
        from vw.core.states import Star

        return self.factories.expr(state=Star(source=self.state), factories=self.factories)
