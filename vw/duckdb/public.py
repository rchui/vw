from vw.core.base import Factories
from vw.core.states import Reference
from vw.postgres.base import Expression, RowSet, SetOperation


def ref(name: str, /) -> RowSet[Expression, RowSet, SetOperation]:
    return RowSet(state=Reference(name=name), factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))
