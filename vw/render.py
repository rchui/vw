"""Rendering infrastructure for SQL generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from strenum import StrEnum

from vw.exceptions import CTENameCollisionError, UnsupportedDialectError

if TYPE_CHECKING:
    from vw.build import CommonTableExpression


class Dialect(StrEnum):
    """Supported SQL dialects for rendering."""

    POSTGRES = "postgres"
    """PostgreSQL style: $param, expr::type"""

    SQLSERVER = "sqlserver"
    """SQL Server style: @param, CAST(expr AS type)"""


@dataclass(kw_only=True, frozen=True)
class RenderConfig:
    """Configuration for SQL rendering."""

    dialect: Dialect = Dialect.POSTGRES


@dataclass(kw_only=True)
class RenderContext:
    """Context for rendering SQL with parameter tracking."""

    config: RenderConfig
    params: dict[str, Any] = field(default_factory=dict)
    ctes: list[tuple[CommonTableExpression, str]] = field(default_factory=list)
    depth: int = 0

    def register_cte(self, cte: CommonTableExpression, body_sql: str) -> None:
        """Register a CTE with its rendered body SQL.

        Args:
            cte: The CommonTableExpression to register.
            body_sql: The pre-rendered SQL body of the CTE.

        Raises:
            CTENameCollisionError: If a different CTE with the same name is already registered.
        """
        for existing_cte, _ in self.ctes:
            if existing_cte is cte:
                return  # Already registered
            if existing_cte.name == cte.name:
                raise CTENameCollisionError(f"CTE name '{cte.name}' defined multiple times")
        self.ctes.append((cte, body_sql))

    def recurse(self) -> RenderContext:
        """Create a child context for nested rendering."""
        return RenderContext(config=self.config, params=self.params, ctes=self.ctes, depth=self.depth + 1)

    def add_param(self, name: str, value: Any) -> str:
        """
        Add a parameter to the context and return its placeholder.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            The SQL placeholder string for this parameter.

        Raises:
            UnsupportedDialectError: If the dialect is not supported.
        """
        self.params[name] = value
        if self.config.dialect == Dialect.POSTGRES:
            return f"${name}"
        elif self.config.dialect == Dialect.SQLSERVER:
            return f"@{name}"
        raise UnsupportedDialectError(f"Unsupported dialect: {self.config.dialect}")


@dataclass(kw_only=True, frozen=True)
class RenderResult:
    """Result of rendering a SQL statement."""

    sql: str
    params: dict[str, Any]
