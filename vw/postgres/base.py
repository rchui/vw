from dataclasses import dataclass

from vw.core.base import Expression as CoreExpression
from vw.core.base import RowSet as CoreRowSet


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(CoreExpression): ...


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(CoreRowSet): ...
