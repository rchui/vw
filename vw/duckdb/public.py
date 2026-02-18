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
from vw.core.mixins import (
    ArrayAggMixin,
    BitAndMixin,
    BitOrMixin,
    BoolAndMixin,
    BoolOrMixin,
    JsonAggMixin,
    StringAggMixin,
    UnnestMixin,
)
from vw.core.states import (
    Exists,
    Literal,
    Parameter,
    Reference,
)
from vw.duckdb.base import Expression, RowSet
from vw.duckdb.states import Column


class Functions(
    ArrayAggMixin,
    UnnestMixin,
    StringAggMixin,
    JsonAggMixin,
    BitAndMixin,
    BitOrMixin,
    BoolAndMixin,
    BoolOrMixin,
    CoreFunctions,
):
    """DuckDB function namespace.

    Inherits ANSI SQL standard functions from CoreFunctions.
    Inherits shared aggregate functions from core mixins.
    DuckDB-specific list functions are added below.
    """

    # --- List Construction Functions --------------------------------------- #

    def list_value(self, *elements: Expression) -> Expression:
        """LIST_VALUE() — create a list from elements.

        Args:
            *elements: Expressions to include in the list.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_value(lit(1), lit(2), lit(3))  # LIST_VALUE(1, 2, 3)
            >>> F.list_value(col("a"), col("b"))  # LIST_VALUE(a, b)
        """
        from vw.core.states import Function

        state = Function(name="LIST_VALUE", args=tuple(e.state for e in elements))
        return self.factories.expr(state=state, factories=self.factories)

    def list_agg(
        self, expr: Expression, *, distinct: bool = False, order_by: list[Expression] | None = None
    ) -> Expression:
        """LIST_AGG() — aggregate values into a list (DuckDB-native).

        Args:
            expr: Expression to aggregate.
            distinct: Whether to aggregate distinct values only.
            order_by: Optional ORDER BY expressions (inside function).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_agg(col("name"))  # LIST_AGG(name)
            >>> F.list_agg(col("name"), distinct=True)  # LIST_AGG(DISTINCT name)
            >>> F.list_agg(col("name"), order_by=[col("name").asc()])
        """
        from vw.core.states import Function

        order_by_tuple = tuple(e.state for e in order_by) if order_by else ()
        state = Function(name="LIST_AGG", args=(expr.state,), distinct=distinct, order_by=order_by_tuple)
        return self.factories.expr(state=state, factories=self.factories)

    # --- List Access Functions --------------------------------------------- #

    def list_extract(self, list_expr: Expression, index: Expression) -> Expression:
        """LIST_EXTRACT() — extract element at index (1-based, negative supported).

        Args:
            list_expr: List expression.
            index: Index expression (1-based, negative indexes from end).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_extract(col("tags"), lit(1))  # First element
            >>> F.list_extract(col("tags"), lit(-1))  # Last element
            >>> F.list_extract(col("tags"), col("idx"))
        """
        from vw.core.states import Function

        state = Function(name="LIST_EXTRACT", args=(list_expr.state, index.state))
        return self.factories.expr(state=state, factories=self.factories)

    def list_slice(
        self, list_expr: Expression, begin: Expression, end: Expression, step: Expression | None = None
    ) -> Expression:
        """LIST_SLICE() — slice list with optional step.

        Args:
            list_expr: List expression to slice.
            begin: Start index (1-based, negative supported).
            end: End index (1-based, negative supported).
            step: Optional step size (default 1).

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_slice(col("items"), lit(1), lit(3))  # items[1:3]
            >>> F.list_slice(col("items"), lit(1), lit(-1))  # All but last
            >>> F.list_slice(col("items"), lit(1), lit(10), lit(2))  # Every 2nd element
        """
        from vw.core.states import Function

        args = [list_expr.state, begin.state, end.state]
        if step is not None:
            args.append(step.state)
        state = Function(name="LIST_SLICE", args=tuple(args))
        return self.factories.expr(state=state, factories=self.factories)

    # --- List Membership Functions ----------------------------------------- #

    def list_contains(self, list_expr: Expression, element: Expression) -> Expression:
        """LIST_CONTAINS() — check if list contains element.

        Args:
            list_expr: List expression to search.
            element: Element to search for.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_contains(col("tags"), lit("important"))
            >>> F.list_contains(col("ids"), col("user_id"))
        """
        from vw.core.states import Function

        state = Function(name="LIST_CONTAINS", args=(list_expr.state, element.state))
        return self.factories.expr(state=state, factories=self.factories)

    # --- List Modification Functions --------------------------------------- #

    def list_concat(self, *lists: Expression) -> Expression:
        """LIST_CONCAT() — concatenate multiple lists.

        Args:
            *lists: List expressions to concatenate.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_concat(col("list1"), col("list2"))
            >>> F.list_concat(col("a"), col("b"), col("c"))
        """
        from vw.core.states import Function

        state = Function(name="LIST_CONCAT", args=tuple(lst.state for lst in lists))
        return self.factories.expr(state=state, factories=self.factories)

    def list_append(self, list_expr: Expression, element: Expression) -> Expression:
        """LIST_APPEND() — append element to end of list.

        Args:
            list_expr: List expression.
            element: Element to append.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_append(col("tags"), lit("new"))
            >>> F.list_append(col("items"), col("next_item"))
        """
        from vw.core.states import Function

        state = Function(name="LIST_APPEND", args=(list_expr.state, element.state))
        return self.factories.expr(state=state, factories=self.factories)

    def list_prepend(self, element: Expression, list_expr: Expression) -> Expression:
        """LIST_PREPEND() — prepend element to beginning of list.

        Args:
            element: Element to prepend.
            list_expr: List expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_prepend(lit("first"), col("tags"))
            >>> F.list_prepend(col("new_item"), col("items"))
        """
        from vw.core.states import Function

        state = Function(name="LIST_PREPEND", args=(element.state, list_expr.state))
        return self.factories.expr(state=state, factories=self.factories)

    # --- List Transformation Functions ------------------------------------- #

    def list_sort(self, list_expr: Expression) -> Expression:
        """LIST_SORT() — sort list in ascending order.

        Args:
            list_expr: List expression to sort.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_sort(col("numbers"))  # LIST_SORT(numbers)
            >>> F.list_sort(col("tags")).alias("sorted_tags")
        """
        from vw.core.states import Function

        state = Function(name="LIST_SORT", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_reverse(self, list_expr: Expression) -> Expression:
        """LIST_REVERSE() — reverse list order.

        Args:
            list_expr: List expression to reverse.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_reverse(col("items"))  # LIST_REVERSE(items)
            >>> F.list_reverse(col("tags")).alias("reversed")
        """
        from vw.core.states import Function

        state = Function(name="LIST_REVERSE", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_distinct(self, list_expr: Expression) -> Expression:
        """LIST_DISTINCT() — remove duplicate elements from list.

        Args:
            list_expr: List expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_distinct(col("tags"))  # LIST_DISTINCT(tags)
            >>> F.list_distinct(col("ids")).alias("unique_ids")
        """
        from vw.core.states import Function

        state = Function(name="LIST_DISTINCT", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_has_any(self, list1: Expression, list2: Expression) -> Expression:
        """LIST_HAS_ANY() — check if lists have any common elements.

        Args:
            list1: First list expression.
            list2: Second list expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_has_any(col("user_tags"), col("required_tags"))
            >>> F.list_has_any(col("permissions"), F.list_value(lit("read"), lit("write")))
        """
        from vw.core.states import Function

        state = Function(name="LIST_HAS_ANY", args=(list1.state, list2.state))
        return self.factories.expr(state=state, factories=self.factories)

    def list_has_all(self, list1: Expression, list2: Expression) -> Expression:
        """LIST_HAS_ALL() — check if first list contains all elements of second list.

        Args:
            list1: List expression to check.
            list2: List of elements to find.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_has_all(col("permissions"), col("required_permissions"))
            >>> F.list_has_all(col("tags"), F.list_value(lit("python"), lit("tutorial")))
        """
        from vw.core.states import Function

        state = Function(name="LIST_HAS_ALL", args=(list1.state, list2.state))
        return self.factories.expr(state=state, factories=self.factories)

    def flatten(self, list_expr: Expression) -> Expression:
        """FLATTEN() — flatten nested lists into a single-level list.

        Args:
            list_expr: Nested list expression to flatten.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.flatten(col("nested_lists"))  # FLATTEN(nested_lists)
            >>> F.flatten(col("matrix")).alias("flat")
        """
        from vw.core.states import Function

        state = Function(name="FLATTEN", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    # --- List Scalar Aggregate Functions ----------------------------------- #

    def list_sum(self, list_expr: Expression) -> Expression:
        """LIST_SUM() — sum all numeric elements in list.

        Args:
            list_expr: List expression with numeric elements.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_sum(col("amounts"))  # LIST_SUM(amounts)
            >>> F.list_sum(col("scores")).alias("total_score")
        """
        from vw.core.states import Function

        state = Function(name="LIST_SUM", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_avg(self, list_expr: Expression) -> Expression:
        """LIST_AVG() — average of all numeric elements in list.

        Args:
            list_expr: List expression with numeric elements.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_avg(col("ratings"))  # LIST_AVG(ratings)
            >>> F.list_avg(col("scores")).alias("avg_score")
        """
        from vw.core.states import Function

        state = Function(name="LIST_AVG", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_min(self, list_expr: Expression) -> Expression:
        """LIST_MIN() — minimum element in list.

        Args:
            list_expr: List expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_min(col("prices"))  # LIST_MIN(prices)
            >>> F.list_min(col("scores")).alias("min_score")
        """
        from vw.core.states import Function

        state = Function(name="LIST_MIN", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_max(self, list_expr: Expression) -> Expression:
        """LIST_MAX() — maximum element in list.

        Args:
            list_expr: List expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_max(col("prices"))  # LIST_MAX(prices)
            >>> F.list_max(col("scores")).alias("max_score")
        """
        from vw.core.states import Function

        state = Function(name="LIST_MAX", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    def list_count(self, list_expr: Expression) -> Expression:
        """LIST_COUNT() — count elements in list (excluding NULL).

        Args:
            list_expr: List expression.

        Returns:
            An Expression wrapping a Function state.

        Examples:
            >>> F.list_count(col("items"))  # LIST_COUNT(items)
            >>> F.list_count(col("tags")).alias("tag_count")
        """
        from vw.core.states import Function

        state = Function(name="LIST_COUNT", args=(list_expr.state,))
        return self.factories.expr(state=state, factories=self.factories)

    # TODO: list_filter/transform/reduce - requires lambda expression support


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
    from typing import cast

    from vw.core.states import CTE, File, RawSource, Reference, SetOperation, Statement, Values

    state = query.state

    # Handle Reference - convenience wrapper (convert to SELECT *)
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
        # Wrap SetOperation/Values/File/RawSource in a CTE
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
