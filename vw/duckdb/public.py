from vw.core.base import Factories
from vw.core.states import Source
from vw.postgres.base import Expression, RowSet, SetOperation


def source(name: str, /) -> RowSet[Expression, RowSet, SetOperation]:
    return RowSet(state=Source(name=name), factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))
