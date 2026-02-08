from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic, TypeVar

from vw.core.protocols import Stateful

if TYPE_CHECKING:
    from vw.core.datetime import DateTimeAccessor
    from vw.core.joins import JoinAccessor
    from vw.core.states import Expr, Reference, SetOperation, Statement, Values
    from vw.core.text import TextAccessor

ExprT = TypeVar("ExprT", bound="Expression")
RowSetT = TypeVar("RowSetT", bound="RowSet")
FactoryT = Generic[ExprT, RowSetT]


@dataclass(eq=False, frozen=True, kw_only=True)
class Factories(FactoryT):
    expr: type[ExprT]
    rowset: type[RowSetT]


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(Stateful, FactoryT):
    state: Expr
    factories: Factories[ExprT, RowSetT]

    # --- Generic Binary Operator ------------------------------------------- #

    def op(self, operator: str, other: ExprT) -> ExprT:
        """Create a generic infix binary operator expression (e.g. op('||', other))."""
        from vw.core.states import Operator, ScalarSubquery, SetOperation, Statement

        if isinstance(other.state, (Statement, SetOperation)):
            right = ScalarSubquery(query=other.state)
        else:
            right = other.state
        return self.factories.expr(
            state=Operator(operator=operator, left=self.state, right=right), factories=self.factories
        )

    # --- Comparison Operators ---------------------------------------------- #

    def __eq__(self, other: ExprT) -> ExprT:  # type: ignore[override]
        """Create an equality comparison (=)."""
        return self.op("=", other)

    def __ne__(self, other: ExprT) -> ExprT:  # type: ignore[override]
        """Create an inequality comparison (<>)."""
        return self.op("<>", other)

    def __lt__(self, other: ExprT) -> ExprT:
        """Create a less than comparison (<)."""
        return self.op("<", other)

    def __le__(self, other: ExprT) -> ExprT:
        """Create a less than or equal comparison (<=)."""
        return self.op("<=", other)

    def __gt__(self, other: ExprT) -> ExprT:
        """Create a greater than comparison (>)."""
        return self.op(">", other)

    def __ge__(self, other: ExprT) -> ExprT:
        """Create a greater than or equal comparison (>=)."""
        return self.op(">=", other)

    # --- Arithmetic Operators ---------------------------------------------- #

    def __add__(self, other: ExprT) -> ExprT:
        """Create an addition expression (+)."""
        return self.op("+", other)

    def __sub__(self, other: ExprT) -> ExprT:
        """Create a subtraction expression (-)."""
        return self.op("-", other)

    def __mul__(self, other: ExprT) -> ExprT:
        """Create a multiplication expression (*)."""
        return self.op("*", other)

    def __truediv__(self, other: ExprT) -> ExprT:
        """Create a division expression (/)."""
        return self.op("/", other)

    def __mod__(self, other: ExprT) -> ExprT:
        """Create a modulo expression (%)."""
        return self.op("%", other)

    # --- Logical Operators ------------------------------------------------- #

    def __and__(self, other: ExprT) -> ExprT:
        """Create a logical AND expression (&)."""
        return self.op("AND", other)

    def __or__(self, other: ExprT) -> ExprT:
        """Create a logical OR expression (|)."""
        return self.op("OR", other)

    def __invert__(self) -> ExprT:
        """Create a logical NOT expression (~)."""
        from vw.core.states import Not

        return self.factories.expr(state=Not(operand=self.state), factories=self.factories)

    # --- Pattern Matching -------------------------------------------------- #

    def like(self, pattern: ExprT, /) -> ExprT:
        """Create a LIKE pattern match expression."""
        from vw.core.states import Like

        return self.factories.expr(state=Like(left=self.state, right=pattern.state), factories=self.factories)

    def not_like(self, pattern: ExprT, /) -> ExprT:
        """Create a NOT LIKE pattern match expression."""
        from vw.core.states import NotLike

        return self.factories.expr(state=NotLike(left=self.state, right=pattern.state), factories=self.factories)

    def is_in(self, *values: ExprT) -> ExprT:
        """Create an IN expression checking membership in a list of values."""
        from vw.core.states import IsIn

        return self.factories.expr(
            state=IsIn(expr=self.state, values=tuple(v.state for v in values)), factories=self.factories
        )

    def is_not_in(self, *values: ExprT) -> ExprT:
        """Create a NOT IN expression checking non-membership in a list of values."""
        from vw.core.states import IsNotIn

        return self.factories.expr(
            state=IsNotIn(expr=self.state, values=tuple(v.state for v in values)), factories=self.factories
        )

    def between(self, lower: ExprT, upper: ExprT, /) -> ExprT:
        """Create a BETWEEN expression checking if value is within range."""
        from vw.core.states import Between

        return self.factories.expr(
            state=Between(expr=self.state, lower_bound=lower.state, upper_bound=upper.state),
            factories=self.factories,
        )

    def not_between(self, lower: ExprT, upper: ExprT, /) -> ExprT:
        """Create a NOT BETWEEN expression checking if value is outside range."""
        from vw.core.states import NotBetween

        return self.factories.expr(
            state=NotBetween(expr=self.state, lower_bound=lower.state, upper_bound=upper.state),
            factories=self.factories,
        )

    # --- NULL Checks ------------------------------------------------------- #

    def is_null(self) -> ExprT:
        """Create an IS NULL check expression."""
        from vw.core.states import IsNull

        return self.factories.expr(state=IsNull(expr=self.state), factories=self.factories)

    def is_not_null(self) -> ExprT:
        """Create an IS NOT NULL check expression."""
        from vw.core.states import IsNotNull

        return self.factories.expr(state=IsNotNull(expr=self.state), factories=self.factories)

    # --- Expression Modifiers ---------------------------------------------- #

    def alias(self, name: str, /) -> ExprT:
        """Create an aliased expression (expr AS name)."""
        from vw.core.states import Alias

        return self.factories.expr(state=Alias(expr=self.state, name=name), factories=self.factories)

    def cast(self, data_type: str, /) -> ExprT:
        """Create a type cast expression."""
        from vw.core.states import Cast

        return self.factories.expr(state=Cast(expr=self.state, data_type=data_type), factories=self.factories)

    def asc(self) -> ExprT:
        """Create an ascending sort order expression."""
        from vw.core.states import Asc

        return self.factories.expr(state=Asc(expr=self.state), factories=self.factories)

    def desc(self) -> ExprT:
        """Create a descending sort order expression."""
        from vw.core.states import Desc

        return self.factories.expr(state=Desc(expr=self.state), factories=self.factories)

    # --- Function Methods -------------------------------------------------- #

    def over(
        self,
        *,
        partition_by: list[ExprT] | None = None,
        order_by: list[ExprT] | None = None,
    ) -> ExprT:
        """Convert function to window function with OVER clause.

        Args:
            partition_by: Expressions to partition by (optional).
            order_by: Expressions to order by (optional).

        Returns:
            An Expression wrapping a WindowFunction state.

        Example:
            >>> F.sum(col("amount")).over(partition_by=[col("customer_id")])
            >>> F.row_number().over(order_by=[col("created_at").desc()])
        """
        from vw.core.states import WindowFunction

        state = WindowFunction(
            function=self.state,
            partition_by=tuple(e.state for e in partition_by) if partition_by else (),
            order_by=tuple(e.state for e in order_by) if order_by else (),
        )
        return self.factories.expr(state=state, factories=self.factories)

    def filter(self, condition: ExprT, /) -> ExprT:
        """Add FILTER (WHERE ...) clause to aggregate function.

        Args:
            condition: Filter condition expression.

        Returns:
            An Expression wrapping a Function state with filter.

        Example:
            >>> F.count().filter(col("status") == param("status", "active"))
        """
        from dataclasses import replace

        state = replace(self.state, filter=condition.state)
        return self.factories.expr(state=state, factories=self.factories)

    def rows_between(self, start: object, end: object, /) -> ExprT:
        """Add ROWS BETWEEN frame clause to window function.

        Args:
            start: Start frame boundary.
            end: End frame boundary.

        Returns:
            An Expression wrapping a WindowFunction state with frame.

        Example:
            >>> from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW
            >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
            ...     UNBOUNDED_PRECEDING, CURRENT_ROW
            ... )
        """
        from dataclasses import replace

        from vw.core.states import FrameClause, WindowFunction

        # Preserve existing exclude if there is one
        existing_exclude = None
        if isinstance(self.state, WindowFunction):
            existing_frame = self.state.frame
            if isinstance(existing_frame, FrameClause):
                existing_exclude = existing_frame.exclude

        frame = FrameClause(mode="ROWS", start=start, end=end, exclude=existing_exclude)
        state = replace(self.state, frame=frame)
        return self.factories.expr(state=state, factories=self.factories)

    def range_between(self, start: object, end: object, /) -> ExprT:
        """Add RANGE BETWEEN frame clause to window function.

        Args:
            start: Start frame boundary.
            end: End frame boundary.

        Returns:
            An Expression wrapping a WindowFunction state with frame.

        Example:
            >>> from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW
            >>> F.sum(col("amount")).over(order_by=[col("date")]).range_between(
            ...     UNBOUNDED_PRECEDING, CURRENT_ROW
            ... )
        """
        from dataclasses import replace

        from vw.core.states import FrameClause, WindowFunction

        # Preserve existing exclude if there is one
        existing_exclude = None
        if isinstance(self.state, WindowFunction):
            existing_frame = self.state.frame
            if isinstance(existing_frame, FrameClause):
                existing_exclude = existing_frame.exclude

        frame = FrameClause(mode="RANGE", start=start, end=end, exclude=existing_exclude)
        state = replace(self.state, frame=frame)
        return self.factories.expr(state=state, factories=self.factories)

    def exclude(self, mode: str, /) -> ExprT:
        """Add EXCLUDE clause to window frame.

        Args:
            mode: Exclude mode ("CURRENT ROW", "GROUP", "TIES", "NO OTHERS").

        Returns:
            An Expression wrapping a WindowFunction state with exclude.

        Example:
            >>> F.sum(col("amount")).over(order_by=[col("date")]).rows_between(
            ...     UNBOUNDED_PRECEDING, CURRENT_ROW
            ... ).exclude("CURRENT ROW")
        """
        from dataclasses import replace

        from vw.core.states import FrameClause, WindowFunction

        # Preserve existing frame or create new one with just exclude
        frame: FrameClause
        if isinstance(self.state, WindowFunction):
            existing_frame = self.state.frame
            if isinstance(existing_frame, FrameClause):
                frame = replace(existing_frame, exclude=mode)
            else:
                frame = FrameClause(mode="ROWS", start=None, end=None, exclude=mode)
        else:
            frame = FrameClause(mode="ROWS", start=None, end=None, exclude=mode)

        state = replace(self.state, frame=frame)
        return self.factories.expr(state=state, factories=self.factories)

    @property
    def text(self) -> TextAccessor[ExprT, RowSetT]:
        """Access string functions on this expression.

        Example:
            >>> col("name").text.upper()
            >>> col("email").text.lower().alias("lower_email")
        """
        from vw.core.text import TextAccessor

        return TextAccessor(self)

    @property
    def dt(self) -> DateTimeAccessor[ExprT, RowSetT]:
        """Access date/time functions on this expression.

        Example:
            >>> col("created_at").dt.extract("year")
            >>> col("ts").dt.extract("epoch")
        """
        from vw.core.datetime import DateTimeAccessor

        return DateTimeAccessor(self)


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(Stateful, FactoryT):
    state: Reference | Statement | SetOperation | Values
    factories: Factories[ExprT, RowSetT]

    def select(self, *columns: ExprT) -> RowSetT:
        """Add columns to SELECT clause.

        Transforms Source → Statement if needed.
        For CTEs, creates new Statement with CTE as source.
        RowSet arguments are automatically wrapped as scalar subqueries.

        Args:
            *columns: Column expressions to select. May include RowSet for scalar subqueries.

        Returns:
            A new RowSet with the columns added.
        """
        from vw.core.states import CTE, Alias, Reference, ScalarSubquery, SetOperation, Statement, Values

        col_states = []
        for c in columns:
            if isinstance(c.state, (Statement, SetOperation)):
                scalar = ScalarSubquery(query=c.state)
                # Promote source-level alias to column alias (e.g. subquery.alias("x"))
                if c.state.alias:
                    col_states.append(Alias(expr=scalar, name=c.state.alias))
                else:
                    col_states.append(scalar)
            else:
                col_states.append(c.state)

        if isinstance(self.state, (Reference, Values)):
            # Transform Source → Statement
            new_state = Statement(source=self.state, columns=tuple(col_states))
        elif isinstance(self.state, CTE):
            # CTE → new Statement with CTE as source
            new_state = Statement(source=self.state, columns=tuple(col_states))
        else:
            # Already Statement, update columns
            new_state = replace(self.state, columns=tuple(col_states))

        return self.factories.rowset(state=new_state, factories=self.factories)

    def alias(self, name: str, /) -> RowSetT:
        """Alias this rowset.

        For Source: sets the table alias.
        For Statement: sets the subquery alias.

        Args:
            name: The alias name.

        Returns:
            A new RowSet with the alias set.
        """
        new_state = replace(self.state, alias=name)
        return self.factories.rowset(state=new_state, factories=self.factories)

    def where(self, *conditions: ExprT) -> RowSetT:
        """Add WHERE clause conditions.

        Transforms Reference or SetOperation → Statement if needed.
        Multiple calls accumulate conditions (combined with AND).

        Args:
            *conditions: Expression conditions to filter rows.

        Returns:
            A new RowSet with WHERE conditions added.
        """
        from vw.core.states import Reference, SetOperation, Statement, Values

        cond_states = tuple(c.state for c in conditions)
        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, where_conditions=cond_states)
        else:
            new_state = replace(
                self.state,
                where_conditions=self.state.where_conditions + cond_states,
            )

        return self.factories.rowset(state=new_state, factories=self.factories)

    def group_by(self, *columns: ExprT) -> RowSetT:
        """Add GROUP BY clause.

        Transforms Reference or SetOperation → Statement if needed.
        Multiple calls replace previous GROUP BY (last wins).

        Args:
            *columns: Column expressions to group by.

        Returns:
            A new RowSet with GROUP BY set.
        """
        from vw.core.states import Reference, SetOperation, Statement, Values

        col_states = tuple(c.state for c in columns)
        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, group_by_columns=col_states)
        else:
            new_state = replace(self.state, group_by_columns=col_states)

        return self.factories.rowset(state=new_state, factories=self.factories)

    def having(self, *conditions: ExprT) -> RowSetT:
        """Add HAVING clause conditions.

        Transforms Reference or SetOperation → Statement if needed.
        Multiple calls accumulate conditions (combined with AND).

        Args:
            *conditions: Expression conditions to filter groups.

        Returns:
            A new RowSet with HAVING conditions added.
        """
        from vw.core.states import Reference, SetOperation, Statement, Values

        cond_states = tuple(c.state for c in conditions)
        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, having_conditions=cond_states)
        else:
            new_state = replace(
                self.state,
                having_conditions=self.state.having_conditions + cond_states,
            )

        return self.factories.rowset(state=new_state, factories=self.factories)

    def order_by(self, *columns: ExprT) -> RowSetT:
        """Add ORDER BY clause.

        Transforms Reference or SetOperation → Statement if needed.
        Multiple calls replace previous ORDER BY (last wins).

        Args:
            *columns: Column expressions to sort by.

        Returns:
            A new RowSet with ORDER BY set.
        """
        from vw.core.states import Reference, SetOperation, Statement, Values

        col_states = tuple(c.state for c in columns)
        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, order_by_columns=col_states)
        else:
            new_state = replace(self.state, order_by_columns=col_states)

        return self.factories.rowset(state=new_state, factories=self.factories)

    def limit(self, count: int, /, *, offset: int | None = None) -> RowSetT:
        """Add LIMIT and optional OFFSET clause.

        Transforms Reference or SetOperation → Statement if needed.
        Multiple calls replace previous LIMIT (last wins).

        Args:
            count: Maximum number of rows to return.
            offset: Number of rows to skip (optional).

        Returns:
            A new RowSet with LIMIT set.
        """
        from vw.core.states import Limit, Reference, SetOperation, Statement, Values

        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, limit=Limit(count=count, offset=offset))
        else:
            new_state = replace(self.state, limit=Limit(count=count, offset=offset))

        return self.factories.rowset(state=new_state, factories=self.factories)

    def distinct(self) -> RowSetT:
        """Add DISTINCT clause to remove duplicate rows.

        Transforms Reference or SetOperation → Statement if needed.

        Returns:
            A new RowSet with DISTINCT set.
        """
        from vw.core.states import Distinct, Reference, SetOperation, Statement, Values

        if isinstance(self.state, (Reference, SetOperation, Values)):
            new_state = Statement(source=self.state, distinct=Distinct())
        else:
            new_state = replace(self.state, distinct=Distinct())

        return self.factories.rowset(state=new_state, factories=self.factories)

    def col(self, name: str, /) -> ExprT:
        """Create a column reference qualified with this rowset's alias or CTE name.

        Preference: alias > CTE name > unqualified

        Args:
            name: Column name.

        Returns:
            An Expression with qualified or unqualified column.
        """
        from vw.core.states import CTE, Column

        if self.state.alias:
            qualified_name = f"{self.state.alias}.{name}"
        elif isinstance(self.state, CTE):
            qualified_name = f"{self.state.name}.{name}"
        else:
            qualified_name = name

        return self.factories.expr(state=Column(name=qualified_name), factories=self.factories)

    @property
    def star(self) -> ExprT:
        """Create a star expression qualified with this rowset's alias or CTE name.

        Preference: alias > CTE name > unqualified

        Returns:
            An Expression with qualified or unqualified star.
        """
        from vw.core.states import CTE, Column

        if self.state.alias:
            star_name = f"{self.state.alias}.*"
        elif isinstance(self.state, CTE):
            star_name = f"{self.state.name}.*"
        else:
            star_name = "*"

        return self.factories.expr(state=Column(name=star_name), factories=self.factories)

    @property
    def join(self) -> JoinAccessor[ExprT, RowSetT]:
        """Access join operations.

        Returns:
            A JoinAccessor for building joins.

        Example:
            >>> users.join.inner(orders, on=[users.col("id") == orders.col("user_id")])
        """
        from vw.core.joins import JoinAccessor

        return JoinAccessor(_rowset=self)

    def __or__(self, other: RowSet[ExprT, RowSetT]) -> RowSetT:
        """UNION operator (deduplicates rows).

        Args:
            other: The right side of the UNION operation.

        Returns:
            A RowSet wrapping a SetOperation state with UNION operator.

        Example:
            >>> users.select(col("id")) | admins.select(col("id"))
        """
        from vw.core.states import SetOperation

        return self.factories.rowset(
            state=SetOperation(left=self.state, operator="UNION", right=other.state),
            factories=self.factories,
        )

    def __add__(self, other: RowSet[ExprT, RowSetT]) -> RowSetT:
        """UNION ALL operator (keeps duplicates).

        Args:
            other: The right side of the UNION ALL operation.

        Returns:
            A RowSet wrapping a SetOperation state with UNION ALL operator.

        Example:
            >>> users.select(col("id")) + admins.select(col("id"))
        """
        from vw.core.states import SetOperation

        return self.factories.rowset(
            state=SetOperation(left=self.state, operator="UNION ALL", right=other.state),
            factories=self.factories,
        )

    def __and__(self, other: RowSet[ExprT, RowSetT]) -> RowSetT:
        """INTERSECT operator.

        Args:
            other: The right side of the INTERSECT operation.

        Returns:
            A RowSet wrapping a SetOperation state with INTERSECT operator.

        Example:
            >>> users.select(col("id")) & banned.select(col("user_id"))
        """
        from vw.core.states import SetOperation

        return self.factories.rowset(
            state=SetOperation(left=self.state, operator="INTERSECT", right=other.state),
            factories=self.factories,
        )

    def __sub__(self, other: RowSet[ExprT, RowSetT]) -> RowSetT:
        """EXCEPT operator.

        Args:
            other: The right side of the EXCEPT operation.

        Returns:
            A RowSet wrapping a SetOperation state with EXCEPT operator.

        Example:
            >>> users.select(col("id")) - banned.select(col("user_id"))
        """
        from vw.core.states import SetOperation

        return self.factories.rowset(
            state=SetOperation(left=self.state, operator="EXCEPT", right=other.state),
            factories=self.factories,
        )
