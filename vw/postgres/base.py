from __future__ import annotations

from dataclasses import dataclass
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
class RowSet(CoreRowSet): ...
