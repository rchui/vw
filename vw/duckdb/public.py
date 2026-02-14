"""DuckDB public API - factory functions and utilities."""

from typing import Any

from vw.core.base import Factories
from vw.core.case import When as When
from vw.core.frame import CURRENT_ROW as CURRENT_ROW
from vw.core.frame import UNBOUNDED_FOLLOWING as UNBOUNDED_FOLLOWING
from vw.core.frame import UNBOUNDED_PRECEDING as UNBOUNDED_PRECEDING
from vw.core.frame import following as following
from vw.core.frame import preceding as preceding
from vw.core.functions import Functions as CoreFunctions
from vw.core.states import (
    Exists,
    Literal,
    Parameter,
    Reference,
)
from vw.duckdb.base import Expression, RowSet
from vw.duckdb.states import Column


class Functions(CoreFunctions):
    """DuckDB function namespace.

    Inherits all ANSI SQL standard functions from CoreFunctions.
    Additional DuckDB-specific functions can be added here in the future.
    """

    pass


# Global Functions instance for DuckDB
F = Functions(Factories(expr=Expression, rowset=RowSet))


def ref(name: str, /) -> RowSet:
    """Create a reference to a table or view.

    Args:
        name: Table or view name.

    Returns:
        A RowSet wrapping a Reference state.

    Example:
        >>> users = ref("users")
        >>> users.select(col("id"), col("name"))
    """
    return RowSet(state=Reference(name=name), factories=Factories(expr=Expression, rowset=RowSet))


def col(name: str, /) -> Expression:
    """Create a column reference.

    Args:
        name: Column name (can include table qualifier like "users.id").

    Returns:
        An Expression wrapping a Column state.

    Example:
        >>> col("name")
        >>> col("users.id")
        >>> col("*")  # SELECT * syntax
    """
    return Expression(state=Column(name=name), factories=Factories(expr=Expression, rowset=RowSet))


def param(name: str, value: object, /) -> Expression:
    """Create a parameter for parameterized queries.

    Args:
        name: Parameter name (will be rendered as $name in DuckDB).
        value: Parameter value.

    Returns:
        An Expression wrapping a Parameter state.

    Example:
        >>> param("min_age", 18)
        >>> ref("users").where(col("age") >= param("min_age", 18))
    """
    return Expression(state=Parameter(name=name, value=value), factories=Factories(expr=Expression, rowset=RowSet))


def lit(value: object, /) -> Expression:
    """Create a literal value (rendered directly in SQL).

    Args:
        value: The literal value (int, float, str, bool, None).

    Returns:
        An Expression wrapping a Literal state.

    Example:
        >>> lit(42)
        >>> lit("hello")
        >>> lit(True)
        >>> lit(None)  # NULL
    """
    return Expression(state=Literal(value=value), factories=Factories(expr=Expression, rowset=RowSet))


def when(condition: Expression, /) -> When[Expression, RowSet]:
    """Start a CASE WHEN expression.

    Args:
        condition: The WHEN condition.

    Returns:
        A When builder for chaining .then() and .otherwise()/.end().

    Example:
        >>> when(col("age") >= lit(18)).then(lit("adult")).otherwise(lit("minor"))
        >>> when(col("status") == lit("active")).then(lit(1)).end()
    """
    return When(condition=condition.state, prior_whens=(), factories=Factories(expr=Expression, rowset=RowSet))


def exists(subquery: RowSet, /) -> Expression:
    """Create an EXISTS subquery check.

    Args:
        subquery: A RowSet to check for existence.

    Returns:
        An Expression wrapping an Exists state.

    Example:
        >>> exists(ref("orders").where(col("user_id") == col("users.id")))
    """
    return Expression(state=Exists(subquery=subquery.state), factories=Factories(expr=Expression, rowset=RowSet))


def values(alias: str, /, *rows: dict[str, Any]) -> RowSet:
    """Create a VALUES clause from an alias and row dictionaries.

    The alias is required because VALUES must always be named when used
    as a row source in FROM, JOIN, or CTE contexts.

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

    CTEs define temporary named result sets using the WITH clause.
    They can be used anywhere a table can be used (FROM, JOIN, subqueries).

    Args:
        name: The name for the CTE.
        query: The query that defines the CTE (must have .select() called or be a set operation).
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
    from vw.core.states import CTE, RawSource, Reference, SetOperation, Values
    from vw.duckdb.states import Star

    state = query.state

    # Handle Reference - convenience wrapper (convert to SELECT *)
    if isinstance(state, Reference):
        star_expr = Expression(state=Star(source=state), factories=query.factories)
        stmt = query.select(star_expr)
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
    elif isinstance(state, (SetOperation, Values, RawSource)):
        # Wrap SetOperation/Values/RawSource in a CTE
        cte_state = CTE(
            name=name,
            recursive=recursive,
            source=state,  # Use source as-is
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

    return RowSet(state=cte_state, factories=Factories(expr=Expression, rowset=RowSet))


def rollup(*columns: Expression) -> Expression:
    """Create a ROLLUP grouping construct.

    Args:
        *columns: Columns to rollup.

    Returns:
        An Expression wrapping a Rollup state.

    Example:
        >>> ref("sales").group_by(rollup(col("year"), col("quarter")))
    """
    from vw.core.states import Rollup

    return Expression(
        state=Rollup(columns=tuple(c.state for c in columns)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def cube(*columns: Expression) -> Expression:
    """Create a CUBE grouping construct.

    Args:
        *columns: Columns to cube.

    Returns:
        An Expression wrapping a Cube state.

    Example:
        >>> ref("sales").group_by(cube(col("year"), col("region")))
    """
    from vw.core.states import Cube

    return Expression(
        state=Cube(columns=tuple(c.state for c in columns)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def grouping_sets(*sets: tuple[Expression, ...]) -> Expression:
    """Create a GROUPING SETS construct.

    Args:
        *sets: Tuples of expressions representing grouping sets.

    Returns:
        An Expression wrapping a GroupingSets state.

    Example:
        >>> grouping_sets(
        ...     (col("year"),),
        ...     (col("quarter"),),
        ...     ()  # Grand total
        ... )
    """
    from vw.core.states import GroupingSets

    converted_sets = tuple(tuple(e.state for e in group) for group in sets)
    return Expression(
        state=GroupingSets(sets=converted_sets),
        factories=Factories(expr=Expression, rowset=RowSet),
    )
