from vw.core.base import Factories
from vw.core.states import Reference
from vw.duckdb.base import Expression, RowSet


def ref(name: str, /) -> RowSet[Expression, RowSet]:
    return RowSet(state=Reference(name=name), factories=Factories(expr=Expression, rowset=RowSet))
