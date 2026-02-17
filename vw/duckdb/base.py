from __future__ import annotations

from dataclasses import dataclass

from vw.core.base import Expression as CoreExpression
from vw.core.base import RowSet as CoreRowSet
from vw.duckdb.star import StarAccessor


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(CoreExpression["Expression", "RowSet"]): ...


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(CoreRowSet["Expression", "RowSet"]):
    @property
    def star(self) -> StarAccessor:
        """Access DuckDB star extensions (EXCLUDE, REPLACE).

        Returns:
            Callable StarAccessor for star expressions.

        Example:
            >>> users.star()                                    # SELECT *
            >>> users.star(users.star.exclude(col("password"))) # SELECT * EXCLUDE (password)
            >>> users.star(users.star.replace(name=F.upper(col("name")))) # SELECT * REPLACE (...)
        """
        return StarAccessor(rowset=self)
