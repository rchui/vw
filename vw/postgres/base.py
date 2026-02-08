from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from vw.core.base import Expression as CoreExpression
from vw.core.base import ExprT, RowSetT
from vw.core.base import RowSet as CoreRowSet

if TYPE_CHECKING:
    from vw.postgres.datetime import PostgresDateTimeAccessor


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(CoreExpression):
    @property
    def dt(self) -> PostgresDateTimeAccessor[ExprT, RowSetT]:
        """Access PostgreSQL date/time functions on this expression.

        Example:
            >>> col("created_at").dt.extract("year")
        """
        from vw.postgres.datetime import PostgresDateTimeAccessor

        return PostgresDateTimeAccessor(self)


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(CoreRowSet):
    def distinct(self, *on: ExprT) -> RowSetT:
        """Add DISTINCT or DISTINCT ON clause.

        Called with no args: SELECT DISTINCT ...
        Called with args: SELECT DISTINCT ON (cols) ... — PostgreSQL only.

        Args:
            *on: Optional expressions for DISTINCT ON deduplication.

        Returns:
            A new RowSet with DISTINCT set.
        """
        from vw.core.states import Distinct, Reference, SetOperation, Statement, Values

        distinct = Distinct(on=tuple(expr.state for expr in on))

        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, distinct=distinct)
        else:
            new_state = replace(self.state, distinct=distinct)

        return self.factories.rowset(state=new_state, factories=self.factories)
