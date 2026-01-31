from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic

from vw.core.base import ExprT


@dataclass(eq=False, frozen=True, kw_only=True)
class Source:
    """Represents a table/view reference."""

    name: str
    alias: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Column:
    """Represents a column reference."""

    name: str
    alias: str | None = None


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
