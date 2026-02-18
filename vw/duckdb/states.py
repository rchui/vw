"""DuckDB-specific state classes."""

from __future__ import annotations

from dataclasses import dataclass, field

from vw.core.states import Column as CoreColumn
from vw.core.states import Expr
from vw.core.states import Star as CoreStar


@dataclass(frozen=True, kw_only=True)
class Column(CoreColumn):
    """DuckDB column reference with star extension support.

    Extends core Column to add DuckDB-specific star modifiers:
    - exclude: Column expressions to exclude from * (SELECT * EXCLUDE (col1, col2))
    - replace: Column replacements for * (SELECT * REPLACE (expr AS col1))

    These modifiers only apply when name="*" or name="table.*".
    """

    exclude: tuple[Expr, ...] | None = None
    replace: dict[str, Expr] | None = None


@dataclass(frozen=True, kw_only=True)
class StarExclude:
    """Represents an EXCLUDE clause for star expressions."""

    columns: tuple[Expr, ...]


@dataclass(frozen=True, kw_only=True)
class StarReplace:
    """Represents a REPLACE clause for star expressions."""

    replacements: dict[str, Expr]


@dataclass(frozen=True, kw_only=True)
class Star(CoreStar):
    """DuckDB star expression with EXCLUDE and REPLACE support.

    Attributes:
        modifiers: Ordered tuple of EXCLUDE/REPLACE clauses to apply
    """

    modifiers: tuple[StarExclude | StarReplace, ...] = field(default_factory=tuple)
