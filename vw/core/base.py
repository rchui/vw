from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic, TypeVar

from vw.core.protocols import Stateful

if TYPE_CHECKING:
    from vw.core.states import ExpressionState, Source, Statement

ExprT = TypeVar("ExprT", bound="Expression")
RowSetT = TypeVar("RowSetT", bound="RowSet")
SetOpT = TypeVar("SetOpT", bound="SetOperation")
FactoryT = Generic[ExprT, RowSetT, SetOpT]


@dataclass(eq=False, frozen=True, kw_only=True)
class Factories(FactoryT):
    expr: type[ExprT]
    rowset: type[RowSetT]
    setop: type[SetOpT]


@dataclass(eq=False, frozen=True, kw_only=True)
class Expression(Stateful, FactoryT):
    state: ExpressionState
    factories: Factories[ExprT, RowSetT, SetOpT]

    # --- Comparison Operators ---------------------------------------------- #

    def __eq__(self, other: ExprT) -> ExprT:  # type: ignore[override]
        """Create an equality comparison (=)."""
        from vw.core.states import Equals

        return self.factories.expr(state=Equals(left=self.state, right=other.state), factories=self.factories)

    def __ne__(self, other: ExprT) -> ExprT:  # type: ignore[override]
        """Create an inequality comparison (<>)."""
        from vw.core.states import NotEquals

        return self.factories.expr(state=NotEquals(left=self.state, right=other.state), factories=self.factories)

    def __lt__(self, other: ExprT) -> ExprT:
        """Create a less than comparison (<)."""
        from vw.core.states import LessThan

        return self.factories.expr(state=LessThan(left=self.state, right=other.state), factories=self.factories)

    def __le__(self, other: ExprT) -> ExprT:
        """Create a less than or equal comparison (<=)."""
        from vw.core.states import LessThanOrEqual

        return self.factories.expr(state=LessThanOrEqual(left=self.state, right=other.state), factories=self.factories)

    def __gt__(self, other: ExprT) -> ExprT:
        """Create a greater than comparison (>)."""
        from vw.core.states import GreaterThan

        return self.factories.expr(state=GreaterThan(left=self.state, right=other.state), factories=self.factories)

    def __ge__(self, other: ExprT) -> ExprT:
        """Create a greater than or equal comparison (>=)."""
        from vw.core.states import GreaterThanOrEqual

        return self.factories.expr(
            state=GreaterThanOrEqual(left=self.state, right=other.state), factories=self.factories
        )

    # --- Arithmetic Operators ---------------------------------------------- #

    def __add__(self, other: ExprT) -> ExprT:
        """Create an addition expression (+)."""
        from vw.core.states import Add

        return self.factories.expr(state=Add(left=self.state, right=other.state), factories=self.factories)

    def __sub__(self, other: ExprT) -> ExprT:
        """Create a subtraction expression (-)."""
        from vw.core.states import Subtract

        return self.factories.expr(state=Subtract(left=self.state, right=other.state), factories=self.factories)

    def __mul__(self, other: ExprT) -> ExprT:
        """Create a multiplication expression (*)."""
        from vw.core.states import Multiply

        return self.factories.expr(state=Multiply(left=self.state, right=other.state), factories=self.factories)

    def __truediv__(self, other: ExprT) -> ExprT:
        """Create a division expression (/)."""
        from vw.core.states import Divide

        return self.factories.expr(state=Divide(left=self.state, right=other.state), factories=self.factories)

    def __mod__(self, other: ExprT) -> ExprT:
        """Create a modulo expression (%)."""
        from vw.core.states import Modulo

        return self.factories.expr(state=Modulo(left=self.state, right=other.state), factories=self.factories)

    # --- Logical Operators ------------------------------------------------- #

    def __and__(self, other: ExprT) -> ExprT:
        """Create a logical AND expression (&)."""
        from vw.core.states import And

        return self.factories.expr(state=And(left=self.state, right=other.state), factories=self.factories)

    def __or__(self, other: ExprT) -> ExprT:
        """Create a logical OR expression (|)."""
        from vw.core.states import Or

        return self.factories.expr(state=Or(left=self.state, right=other.state), factories=self.factories)

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


@dataclass(eq=False, frozen=True, kw_only=True)
class RowSet(Stateful, FactoryT):
    state: Source | Statement[ExprT]
    factories: Factories[ExprT, RowSetT, SetOpT]

    def select(self, *columns: ExprT) -> RowSetT:
        """Add columns to SELECT clause.

        Transforms Source → Statement if needed.

        Args:
            *columns: Column expressions to select.

        Returns:
            A new RowSet with the columns added.
        """
        from vw.core.states import Source, Statement

        if isinstance(self.state, Source):
            # Transform Source → Statement
            new_state = Statement(source=self.state, columns=tuple(columns))
        else:
            # Already Statement, update columns
            new_state = replace(self.state, columns=tuple(columns))

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

        Transforms Source → Statement if needed.
        Multiple calls accumulate conditions (combined with AND).

        Args:
            *conditions: Expression conditions to filter rows.

        Returns:
            A new RowSet with WHERE conditions added.
        """
        from vw.core.states import Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, where_conditions=tuple(conditions))
        else:
            new_state = replace(
                self.state,
                where_conditions=self.state.where_conditions + tuple(conditions),
            )

        return self.factories.rowset(state=new_state, factories=self.factories)

    def group_by(self, *columns: ExprT) -> RowSetT:
        """Add GROUP BY clause.

        Transforms Source → Statement if needed.
        Multiple calls replace previous GROUP BY (last wins).

        Args:
            *columns: Column expressions to group by.

        Returns:
            A new RowSet with GROUP BY set.
        """
        from vw.core.states import Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, group_by_columns=tuple(columns))
        else:
            new_state = replace(self.state, group_by_columns=tuple(columns))

        return self.factories.rowset(state=new_state, factories=self.factories)

    def having(self, *conditions: ExprT) -> RowSetT:
        """Add HAVING clause conditions.

        Transforms Source → Statement if needed.
        Multiple calls accumulate conditions (combined with AND).

        Args:
            *conditions: Expression conditions to filter groups.

        Returns:
            A new RowSet with HAVING conditions added.
        """
        from vw.core.states import Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, having_conditions=tuple(conditions))
        else:
            new_state = replace(
                self.state,
                having_conditions=self.state.having_conditions + tuple(conditions),
            )

        return self.factories.rowset(state=new_state, factories=self.factories)

    def order_by(self, *columns: ExprT) -> RowSetT:
        """Add ORDER BY clause.

        Transforms Source → Statement if needed.
        Multiple calls replace previous ORDER BY (last wins).

        Args:
            *columns: Column expressions to sort by.

        Returns:
            A new RowSet with ORDER BY set.
        """
        from vw.core.states import Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, order_by_columns=tuple(columns))
        else:
            new_state = replace(self.state, order_by_columns=tuple(columns))

        return self.factories.rowset(state=new_state, factories=self.factories)

    def limit(self, count: int, /, *, offset: int | None = None) -> RowSetT:
        """Add LIMIT and optional OFFSET clause.

        Transforms Source → Statement if needed.
        Multiple calls replace previous LIMIT (last wins).

        Args:
            count: Maximum number of rows to return.
            offset: Number of rows to skip (optional).

        Returns:
            A new RowSet with LIMIT set.
        """
        from vw.core.states import Limit, Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, limit=Limit(count=count, offset=offset))
        else:
            new_state = replace(self.state, limit=Limit(count=count, offset=offset))

        return self.factories.rowset(state=new_state, factories=self.factories)

    def distinct(self) -> RowSetT:
        """Add DISTINCT clause to remove duplicate rows.

        Transforms Source → Statement if needed.

        Returns:
            A new RowSet with DISTINCT set.
        """
        from vw.core.states import Distinct, Source, Statement

        if isinstance(self.state, Source):
            new_state = Statement(source=self.state, distinct=Distinct())
        else:
            new_state = replace(self.state, distinct=Distinct())

        return self.factories.rowset(state=new_state, factories=self.factories)

    def col(self, name: str, /) -> ExprT:
        """Create a column reference qualified with this rowset's alias.

        Args:
            name: Column name.

        Returns:
            An Expression with qualified or unqualified column.
        """
        from vw.core.states import Column

        if self.state.alias:
            qualified_name = f"{self.state.alias}.{name}"
        else:
            qualified_name = name

        return self.factories.expr(state=Column(name=qualified_name), factories=self.factories)

    @property
    def star(self) -> ExprT:
        """Create a star expression qualified with this rowset's alias.

        Returns:
            An Expression with qualified or unqualified star.
        """
        from vw.core.states import Column

        if self.state.alias:
            star_name = f"{self.state.alias}.*"
        else:
            star_name = "*"

        return self.factories.expr(state=Column(name=star_name), factories=self.factories)


@dataclass(eq=False, frozen=True, kw_only=True)
class SetOperation(RowSet): ...
