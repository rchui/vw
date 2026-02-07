from vw.core.base import Factories
from vw.core.frame import CURRENT_ROW as CURRENT_ROW
from vw.core.frame import UNBOUNDED_FOLLOWING as UNBOUNDED_FOLLOWING
from vw.core.frame import UNBOUNDED_PRECEDING as UNBOUNDED_PRECEDING
from vw.core.frame import following as following
from vw.core.frame import preceding as preceding
from vw.core.functions import Functions as CoreFunctions
from vw.core.states import Column, Exists, Parameter, Reference
from vw.postgres.base import Expression, RowSet, SetOperation


class Functions(CoreFunctions):
    """PostgreSQL function namespace.

    Inherits all ANSI SQL standard functions from CoreFunctions.
    PostgreSQL-specific functions can be added here in the future.
    """

    pass


# Instantiate with PostgreSQL factories
F = Functions(factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))


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
    return Expression(
        state=Exists(subquery=subquery.state), factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation)
    )


def cte(name: str, query: RowSet, /, *, recursive: bool = False) -> RowSet:
    """Create a Common Table Expression (CTE).

    CTEs define temporary named result sets using the WITH clause.
    They can be used anywhere a table can be used (FROM, JOIN, subqueries).

    Args:
        name: The name for the CTE.
        query: The query that defines the CTE (must have .select() called or be a SetOperation).
        recursive: If True, creates WITH RECURSIVE for self-referencing CTEs.

    Returns:
        A RowSet that can be used like a table.

    Example:
        >>> active_users = cte(
        ...     "active_users",
        ...     ref("users").select(col("*")).where(col("active") == True)
        ... )
        >>> result = active_users.select(col("id"), col("name"))
        # WITH active_users AS (SELECT * FROM users WHERE active = true)
        # SELECT id, name FROM active_users

    Recursive Example:
        >>> # Anchor: top-level items
        >>> anchor = ref("items").select(col("*")).where(col("parent_id").is_null())
        >>> tree = cte("tree", anchor, recursive=True)
        >>> # Recursive part
        >>> recursive_part = tree.alias("t").join.inner(
        ...     ref("items").alias("i"),
        ...     on=[col("i.parent_id") == col("t.id")]
        ... ).select(col("i.*"))
        >>> # Final CTE with UNION ALL
        >>> tree = cte("tree", anchor + recursive_part, recursive=True)
    """
    from vw.core.states import CTE, Reference, SetOperationState

    state = query.state

    # Handle Reference - convenience wrapper (convert to SELECT *)
    if isinstance(state, Reference):
        stmt = query.select(col("*"))
        stmt_state = stmt.state
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=stmt_state.source,
            alias=stmt_state.alias,
            columns=stmt_state.columns,
            where_conditions=stmt_state.where_conditions,
            group_by_columns=stmt_state.group_by_columns,
            having_conditions=stmt_state.having_conditions,
            order_by_columns=stmt_state.order_by_columns,
            limit=stmt_state.limit,
            distinct=stmt_state.distinct,
            joins=stmt_state.joins,
        )
    elif isinstance(state, SetOperationState):
        # Wrap SetOperationState in a CTE
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=state,  # Use SetOperationState as source
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
        # Type narrowing: state is now Statement
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

    return RowSet(state=cte_state, factories=Factories(expr=Expression, rowset=RowSet, setop=SetOperation))
