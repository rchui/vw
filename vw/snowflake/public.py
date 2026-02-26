from typing import Any

from vw.core.base import Factories
from vw.core.case import When as When
from vw.core.frame import CURRENT_ROW as CURRENT_ROW
from vw.core.frame import UNBOUNDED_FOLLOWING as UNBOUNDED_FOLLOWING
from vw.core.frame import UNBOUNDED_PRECEDING as UNBOUNDED_PRECEDING
from vw.core.frame import following as following
from vw.core.frame import preceding as preceding
from vw.core.functions import Functions as CoreFunctions
from vw.core.states import Column, Exists, Parameter, Reference
from vw.snowflake.base import Expression, RowSet


class Functions(CoreFunctions):
    """Snowflake function namespace.

    Inherits ANSI SQL standard functions from CoreFunctions.
    Snowflake-specific aggregate and scalar functions will be added in later phases.
    """


# Instantiate with Snowflake factories
F = Functions(factories=Factories(expr=Expression, rowset=RowSet))


def ref(name: str, /) -> RowSet:
    """Create a table/view reference.

    Args:
        name: The table or view name.

    Returns:
        A RowSet wrapping a Reference.

    Example:
        >>> ref("users")
    """
    return RowSet(
        state=Reference(name=name),
        factories=Factories(expr=Expression, rowset=RowSet),
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
    return Expression(state=Column(name=name), factories=Factories(expr=Expression, rowset=RowSet))


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
    return Expression(state=Parameter(name=name, value=value), factories=Factories(expr=Expression, rowset=RowSet))


def lit(value: object, /) -> Expression:
    """Create a literal value expression.

    Literals are compile-time constants rendered directly in SQL with proper
    escaping for SQL injection safety. Strings are quoted and escaped ('active'),
    numbers rendered as-is (42), booleans as TRUE/FALSE, None as NULL.

    Args:
        value: The literal value (any type the driver can handle).

    Returns:
        An Expression wrapping a Literal state.

    Examples:
        >>> col("status") == lit("active")
        >>> col("priority") > lit(5)
    """
    from vw.core.states import Literal

    return Expression(state=Literal(value=value), factories=Factories(expr=Expression, rowset=RowSet))


def when(condition: Expression, /) -> When[Expression, RowSet]:
    """Start a CASE WHEN expression.

    Args:
        condition: The boolean condition to check.

    Returns:
        A When builder that must be completed with .then().

    Example:
        >>> when(col("status") == param("a", "active")).then(param("one", 1))
        ...     .when(col("status") == param("i", "inactive")).then(param("zero", 0))
        ...     .otherwise(param("default", -1))
    """
    from vw.core.case import When as WhenBuilder

    return WhenBuilder(
        condition=condition.state,
        prior_whens=(),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def exists(subquery: RowSet, /) -> Expression:
    """Create an EXISTS subquery check.

    Args:
        subquery: The subquery to check for existence.

    Returns:
        An Expression wrapping an Exists state.

    Example:
        >>> users = ref("users")
        >>> orders = ref("orders")
        >>> users.where(exists(orders.where(orders.col("user_id") == users.col("id"))))
    """
    return Expression(state=Exists(subquery=subquery.state), factories=Factories(expr=Expression, rowset=RowSet))


def values(alias: str, /, *rows: dict[str, Any]) -> RowSet:
    """Create a VALUES clause from an alias and row dictionaries.

    Args:
        alias: The name for the VALUES source.
        *rows: Row dictionaries where keys are column names.

    Returns:
        A RowSet wrapping a Values state.

    Example:
        >>> values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
        ...     .select(col("id"), col("name"))
    """
    from vw.core.states import Values

    unwrapped = tuple({k: v.state if isinstance(v, Expression) else v for k, v in row.items()} for row in rows)
    return RowSet(
        state=Values(rows=unwrapped, alias=alias),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def cte(name: str, query: RowSet, /, *, recursive: bool = False) -> RowSet:
    """Create a Common Table Expression (CTE).

    Args:
        name: The name for the CTE.
        query: The query that defines the CTE.
        recursive: If True, creates WITH RECURSIVE for self-referencing CTEs.

    Returns:
        A RowSet that can be used like a table.

    Example:
        >>> active_users = cte(
        ...     "active_users",
        ...     ref("users").select(col("*")).where(col("active") == True)
        ... )
        >>> result = active_users.select(col("id"), col("name"))
    """
    from typing import cast

    from vw.core.states import CTE, File, RawSource, Reference, SetOperation, Statement, Values

    state = query.state

    if isinstance(state, Reference):
        stmt = cast(Statement, query.select(query.star()).state)
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=stmt.source,
            alias=stmt.alias,
            columns=stmt.columns,
            where_conditions=stmt.where_conditions,
            group_by_columns=stmt.group_by_columns,
            having_conditions=stmt.having_conditions,
            order_by_columns=stmt.order_by_columns,
            limit=stmt.limit,
            distinct=stmt.distinct,
            joins=stmt.joins,
        )
    elif isinstance(state, (SetOperation, Values, File, RawSource)):
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=state,
            alias=None,
            columns=(),
            where_conditions=(),
            group_by_columns=(),
            having_conditions=(),
            order_by_columns=(),
            limit=None,
            distinct=None,
            joins=(),
        )
    else:
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=state.source,
            alias=state.alias,
            columns=state.columns,
            where_conditions=state.where_conditions,
            group_by_columns=state.group_by_columns,
            having_conditions=state.having_conditions,
            order_by_columns=state.order_by_columns,
            limit=state.limit,
            distinct=state.distinct,
            joins=state.joins,
        )

    return RowSet(state=cte_state, factories=Factories(expr=Expression, rowset=RowSet))


def rollup(*columns: Expression) -> Expression:
    """Create a ROLLUP grouping construct for hierarchical subtotals.

    Args:
        *columns: Columns to include in the ROLLUP.

    Returns:
        An Expression wrapping a Rollup state.
    """
    from vw.core.states import Rollup

    return Expression(
        state=Rollup(columns=tuple(c.state for c in columns)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def cube(*columns: Expression) -> Expression:
    """Create a CUBE grouping construct for all dimension combinations.

    Args:
        *columns: Columns to include in the CUBE.

    Returns:
        An Expression wrapping a Cube state.
    """
    from vw.core.states import Cube

    return Expression(
        state=Cube(columns=tuple(c.state for c in columns)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def grouping_sets(*sets: tuple[Expression, ...]) -> Expression:
    """Create a GROUPING SETS construct for explicit grouping combinations.

    Args:
        *sets: Tuples of column expressions defining each grouping set.
               Use an empty tuple () for the grand total row.

    Returns:
        An Expression wrapping a GroupingSets state.
    """
    from vw.core.states import GroupingSets

    return Expression(
        state=GroupingSets(sets=tuple(tuple(e.state for e in s) for s in sets)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def interval(amount: int | float, unit: str, /) -> Expression:
    """Create a Snowflake INTERVAL literal expression.

    Args:
        amount: The quantity of time units.
        unit: The time unit (e.g. "day", "hour", "month", "year").

    Returns:
        An Expression wrapping an Interval state.

    Example:
        >>> col("created_at") + interval(1, "day")
        >>> col("expires_at") - interval(30, "day")
    """
    from vw.core.states import Interval

    return Expression(state=Interval(amount=amount, unit=unit), factories=Factories(expr=Expression, rowset=RowSet))
