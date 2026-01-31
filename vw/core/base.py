from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from vw.core.protocols import Stateful

if TYPE_CHECKING:
    from vw.core.states import Source, Statement

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
class Expression(Stateful):
    state: Any  # Column or other expression types
    factories: Factories[ExprT, RowSetT, SetOpT]


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
