from vw.core.base import Factories
from vw.core.states import Column, Source
from vw.postgres.base import Expression, RowSet, SetOperation


def source(name: str, /) -> RowSet:
    """Create a table/view source.

    Args:
        name: The table or view name.

    Returns:
        A RowSet wrapping a Source.

    Example:
        >>> source("users")
    """
    return RowSet(
        state=Source(name=name),
        factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation),
    )


def col(name: str, /) -> Expression:
    """Create a column expression.

    Args:
        name: The column name (can be qualified like "users.id").

    Returns:
        An Expression wrapping a Column.

    Example:
        >>> col("id")
        >>> col("users.id")
    """
    return Expression(state=Column(name=name), factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))
