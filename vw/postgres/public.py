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
from vw.postgres.base import Expression, RowSet


class Functions(CoreFunctions):
    """PostgreSQL function namespace.

    Inherits all ANSI SQL standard functions from CoreFunctions.
    """

    def now(self) -> Expression:
        """NOW() — returns current timestamp (PostgreSQL-specific)."""
        from vw.postgres.states import Now

        return self.factories.expr(state=Now(), factories=self.factories)

    def gen_random_uuid(self) -> Expression:
        """GEN_RANDOM_UUID() — generate a random UUID v4.

        Example:
            >>> ref("users").select(F.gen_random_uuid().alias("id"))
        """
        from vw.core.states import Function

        state = Function(name="GEN_RANDOM_UUID", args=())
        return self.factories.expr(state=state, factories=self.factories)

    def array_agg(
        self, expr: Expression, *, distinct: bool = False, order_by: list[Expression] | None = None
    ) -> Expression:
        """ARRAY_AGG() — aggregate values into an array.

        Args:
            expr: Expression to aggregate.
            distinct: Whether to aggregate distinct values only.
            order_by: Optional ORDER BY expressions (inside function).

        Examples:
            >>> F.array_agg(col("name"))
            >>> F.array_agg(col("name"), distinct=True)
            >>> F.array_agg(col("name"), order_by=[col("name").asc()])
            >>> F.array_agg(col("tag"), distinct=True, order_by=[col("tag")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="ARRAY_AGG", args=(expr.state,), distinct=distinct, order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)

    def string_agg(
        self, expr: Expression, separator: Expression, *, order_by: list[Expression] | None = None
    ) -> Expression:
        """STRING_AGG() — concatenate values with separator.

        Args:
            expr: Expression to aggregate.
            separator: Separator string (typically lit() for constants).
            order_by: Optional ORDER BY expressions (inside function).

        Examples:
            >>> F.string_agg(col("name"), lit(", "))
            >>> F.string_agg(col("name"), lit(", "), order_by=[col("name")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="STRING_AGG", args=(expr.state, separator.state), order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)

    def json_build_object(self, *args: Expression) -> Expression:
        """JSON_BUILD_OBJECT() — build JSON object from key/value pairs.

        Args:
            *args: Alternating key and value expressions.
                  Use lit() for string keys.

        Example:
            >>> F.json_build_object(
            ...     lit("id"), col("id"),
            ...     lit("name"), col("name")
            ... )
        """
        from vw.core.states import Function

        state = Function(name="JSON_BUILD_OBJECT", args=tuple(a.state for a in args))
        return self.factories.expr(state=state, factories=self.factories)

    def json_agg(self, expr: Expression, *, order_by: list[Expression] | None = None) -> Expression:
        """JSON_AGG() — aggregate values into a JSON array.

        Args:
            expr: Expression to aggregate.
            order_by: Optional ORDER BY expressions (inside function).

        Example:
            >>> F.json_agg(col("data"), order_by=[col("created_at")])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="JSON_AGG", args=(expr.state,), order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)

    def unnest(self, array: Expression) -> Expression:
        """UNNEST() — expand array to set of rows.

        Can be used in SELECT clause. For FROM clause usage, use raw.rowset().

        Example:
            >>> ref("data").select(F.unnest(col("array_col")))
            >>> # For FROM: raw.rowset("unnest({arr}) AS t(elem)", arr=...)
        """
        from vw.core.states import Function

        state = Function(name="UNNEST", args=(array.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def bit_and(self, expr: Expression) -> Expression:
        """BIT_AND(expr) — Bitwise AND aggregate.

        Computes the bitwise AND of all non-null input values.

        Args:
            expr: Expression to aggregate (typically an integer column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bit_and(col("flags"))  # BIT_AND(flags)
            >>> F.bit_and(col("permissions")).alias("combined_perms")
        """
        from vw.core.states import Function

        state = Function(name="BIT_AND", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def bit_or(self, expr: Expression) -> Expression:
        """BIT_OR(expr) — Bitwise OR aggregate.

        Computes the bitwise OR of all non-null input values.

        Args:
            expr: Expression to aggregate (typically an integer column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bit_or(col("flags"))  # BIT_OR(flags)
            >>> F.bit_or(col("permissions")).alias("combined_perms")
        """
        from vw.core.states import Function

        state = Function(name="BIT_OR", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def bool_and(self, expr: Expression) -> Expression:
        """BOOL_AND(expr) — Boolean AND aggregate (also known as EVERY).

        Returns true if all input values are true, otherwise false.

        Args:
            expr: Expression to aggregate (boolean expression or column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bool_and(col("is_active"))  # BOOL_AND(is_active)
            >>> F.bool_and(col("verified")).alias("all_verified")
        """
        from vw.core.states import Function

        state = Function(name="BOOL_AND", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def bool_or(self, expr: Expression) -> Expression:
        """BOOL_OR(expr) — Boolean OR aggregate.

        Returns true if at least one input value is true, otherwise false.

        Args:
            expr: Expression to aggregate (boolean expression or column).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.bool_or(col("needs_review"))  # BOOL_OR(needs_review)
            >>> F.bool_or(col("has_error")).alias("any_errors")
        """
        from vw.core.states import Function

        state = Function(name="BOOL_OR", args=(expr.state,))
        return self.factories.expr(state=state, factories=self.factories)


# Instantiate with PostgreSQL factories
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

    Use lit() for: JSON keys, separators, status strings, magic numbers.
    Use param() for: user input, runtime values (self-documenting).

    Args:
        value: The literal value (any type the driver can handle).

    Returns:
        An Expression wrapping a Literal state.

    Examples:
        >>> F.json_build_object(lit("id"), col("id"), lit("name"), col("name"))
        >>> F.string_agg(col("tag"), lit(", "))
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
    """Create a ROLLUP grouping construct for hierarchical subtotals.

    Args:
        *columns: Columns to include in the ROLLUP.

    Returns:
        An Expression wrapping a Rollup state.

    Example:
        >>> ref("sales").select(col("region"), col("product"), F.sum(col("amount")))
        ...     .group_by(rollup(col("region"), col("product")))
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

    Example:
        >>> ref("sales").select(col("region"), col("product"), F.sum(col("amount")))
        ...     .group_by(cube(col("region"), col("product")))
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

    Example:
        >>> ref("sales").select(col("region"), col("product"), F.sum(col("amount")))
        ...     .group_by(grouping_sets(
        ...         (col("region"), col("product")),
        ...         (col("region"),),
        ...         (),
        ...     ))
    """
    from vw.core.states import GroupingSets

    return Expression(
        state=GroupingSets(sets=tuple(tuple(e.state for e in s) for s in sets)),
        factories=Factories(expr=Expression, rowset=RowSet),
    )


def interval(amount: int | float, unit: str, /) -> Expression:
    """Create a PostgreSQL INTERVAL literal expression.

    Args:
        amount: The quantity of time units.
        unit: The time unit (e.g. "day", "hour", "month", "year").

    Returns:
        An Expression wrapping an Interval state.

    Example:
        >>> col("created_at") + interval(1, "day")
        >>> col("expires_at") - interval(30, "day")
    """
    from vw.postgres.states import Interval

    return Expression(state=Interval(amount=amount, unit=unit), factories=Factories(expr=Expression, rowset=RowSet))
