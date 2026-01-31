from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic

from vw.core.base import ExprT

# --- Base Classes ---------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class ExpressionState:
    """Base class for all expression states."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Source:
    """Represents a table/view reference."""

    name: str
    alias: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Column(ExpressionState):
    """Represents a column reference."""

    name: str
    alias: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Parameter(ExpressionState):
    """Represents a query parameter."""

    name: str
    value: object


@dataclass(eq=False, frozen=True, kw_only=True)
class Limit:
    """Represents LIMIT and optional OFFSET for pagination."""

    count: int
    offset: int | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Distinct:
    """Represents DISTINCT clause in a SQL statement."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Statement(Generic[ExprT]):
    """Represents a SELECT query."""

    source: Source | Statement
    alias: str | None = None
    columns: tuple[ExprT, ...] = field(default_factory=tuple)
    where_conditions: tuple[ExprT, ...] = field(default_factory=tuple)
    group_by_columns: tuple[ExprT, ...] = field(default_factory=tuple)
    having_conditions: tuple[ExprT, ...] = field(default_factory=tuple)
    order_by_columns: tuple[ExprT, ...] = field(default_factory=tuple)
    limit: Limit | None = None
    distinct: Distinct | None = None


# --- Comparison Operators -------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Equals(ExpressionState):
    """Represents an equality comparison (=)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class NotEquals(ExpressionState):
    """Represents an inequality comparison (<>)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class LessThan(ExpressionState):
    """Represents a less than comparison (<)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class LessThanOrEqual(ExpressionState):
    """Represents a less than or equal comparison (<=)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class GreaterThan(ExpressionState):
    """Represents a greater than comparison (>)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class GreaterThanOrEqual(ExpressionState):
    """Represents a greater than or equal comparison (>=)."""

    left: ExpressionState
    right: ExpressionState


# --- Arithmetic Operators -------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Add(ExpressionState):
    """Represents addition (+)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Subtract(ExpressionState):
    """Represents subtraction (-)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Multiply(ExpressionState):
    """Represents multiplication (*)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Divide(ExpressionState):
    """Represents division (/)."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Modulo(ExpressionState):
    """Represents modulo (%)."""

    left: ExpressionState
    right: ExpressionState


# --- Logical Operators ----------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class And(ExpressionState):
    """Represents logical AND."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Or(ExpressionState):
    """Represents logical OR."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Not(ExpressionState):
    """Represents logical NOT."""

    operand: ExpressionState


# --- Pattern Matching ------------------------------------------------------ #


@dataclass(eq=False, frozen=True, kw_only=True)
class Like(ExpressionState):
    """Represents LIKE pattern match."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class NotLike(ExpressionState):
    """Represents NOT LIKE pattern match."""

    left: ExpressionState
    right: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class IsIn(ExpressionState):
    """Represents IN check against list of values or subquery."""

    expr: ExpressionState
    values: tuple[ExpressionState, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNotIn(ExpressionState):
    """Represents NOT IN check against list of values or subquery."""

    expr: ExpressionState
    values: tuple[ExpressionState, ...]


@dataclass(eq=False, frozen=True, kw_only=True)
class Between(ExpressionState):
    """Represents BETWEEN check for value within range."""

    expr: ExpressionState
    lower_bound: ExpressionState
    upper_bound: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class NotBetween(ExpressionState):
    """Represents NOT BETWEEN check for value outside range."""

    expr: ExpressionState
    lower_bound: ExpressionState
    upper_bound: ExpressionState


# --- NULL Checks ----------------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNull(ExpressionState):
    """Represents IS NULL check."""

    expr: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class IsNotNull(ExpressionState):
    """Represents IS NOT NULL check."""

    expr: ExpressionState


# --- Expression Modifiers -------------------------------------------------- #


@dataclass(eq=False, frozen=True, kw_only=True)
class Alias(ExpressionState):
    """Represents an aliased expression (expr AS name)."""

    expr: ExpressionState
    name: str


@dataclass(eq=False, frozen=True, kw_only=True)
class Cast(ExpressionState):
    """Represents type cast (CAST or ::)."""

    expr: ExpressionState
    data_type: str


@dataclass(eq=False, frozen=True, kw_only=True)
class Asc(ExpressionState):
    """Represents ascending sort order (ASC)."""

    expr: ExpressionState


@dataclass(eq=False, frozen=True, kw_only=True)
class Desc(ExpressionState):
    """Represents descending sort order (DESC)."""

    expr: ExpressionState
