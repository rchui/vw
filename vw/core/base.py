from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Generic, TypeVar

from vw.core.protocols import Stateful

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
    state: Any  # Source or Statement
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


@dataclass(eq=False, frozen=True, kw_only=True)
class SetOperation(RowSet): ...
