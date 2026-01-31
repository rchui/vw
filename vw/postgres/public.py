from vw.core.base import Factories
from vw.core.frame import (
    CURRENT_ROW as CURRENT_ROW,
)
from vw.core.frame import (
    UNBOUNDED_FOLLOWING as UNBOUNDED_FOLLOWING,
)
from vw.core.frame import (
    UNBOUNDED_PRECEDING as UNBOUNDED_PRECEDING,
)
from vw.core.frame import (
    following as following,
)
from vw.core.frame import (
    preceding as preceding,
)
from vw.core.functions import Functions as CoreFunctions
from vw.core.states import Column, Parameter, Source
from vw.postgres.base import Expression, RowSet, SetOperation


class Functions(CoreFunctions):
    """PostgreSQL function namespace.

    Inherits all ANSI SQL standard functions from CoreFunctions.
    PostgreSQL-specific functions can be added here in the future.
    """

    pass


# Instantiate with PostgreSQL factories
F = Functions(factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))


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


def param(name: str, value: object, /) -> Expression:
    """Create a parameter expression.

    Args:
        name: The parameter name (for documentation/debugging).
        value: The parameter value.

    Returns:
        An Expression wrapping a Parameter.

    Example:
        >>> param("min_age", 18)
        >>> param("status", "active")
        >>> param("enabled", True)
    """
    return Expression(
        state=Parameter(name=name, value=value), factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation)
    )
