"""Rendering infrastructure for SQL generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from strenum import StrEnum

from vw.reference.exceptions import (
    CTENameCollisionError,
    UnsupportedDialectError,
    UnsupportedParamStyleError,
)

if TYPE_CHECKING:
    from vw.reference.build import CommonTableExpression


class ParamStyle(StrEnum):
    """Supported parameter styles for rendering."""

    COLON = "colon"  # :param_name
    DOLLAR = "dollar"  # $param_name
    AT = "at"  # @param_name
    PYFORMAT = "pyformat"  # %(param_name)s


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
    param_style: ParamStyle | None = None


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

    def render_ctes(self) -> str | None:
        """Render the WITH clause if CTEs were registered.

        Returns:
            The WITH [RECURSIVE] clause string, or None if no CTEs.
        """
        if not self.ctes:
            return None
        cte_definitions = [f"{cte.name} AS {body_sql}" for cte, body_sql in self.ctes]
        has_recursive = any(cte._recursive for cte, _ in self.ctes)
        with_keyword = "WITH RECURSIVE" if has_recursive else "WITH"
        return f"{with_keyword} {', '.join(cte_definitions)}"

    def add_param(self, name: str, value: Any) -> str:
        """
        Add a parameter to the context and return its placeholder.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            The SQL placeholder string for this parameter.

        Raises:
            UnsupportedDialectError: If the dialect is not supported for default styling.
            UnsupportedParamStyleError: If the parameter style is not supported.
        """
        self.params[name] = value

        # User specified parameter style takes precedence
        if self.config.param_style:
            if self.config.param_style == ParamStyle.DOLLAR:
                return f"${name}"
            elif self.config.param_style == ParamStyle.AT:
                return f"@{name}"
            elif self.config.param_style == ParamStyle.COLON:
                return f":{name}"
            elif self.config.param_style == ParamStyle.PYFORMAT:
                return f"%({name})s"
            else:
                raise UnsupportedParamStyleError(f"Unsupported parameter style: {self.config.param_style}")

        # Default parameter style based on dialect
        if self.config.dialect == Dialect.POSTGRES:
            return f"${name}"
        elif self.config.dialect == Dialect.SQLSERVER:
            return f"@{name}"
        else:
            raise UnsupportedDialectError(f"Unsupported dialect for parameter styling: {self.config.dialect}")


@dataclass(kw_only=True, frozen=True)
class RenderResult:
    """Result of rendering a SQL statement."""

    sql: str
    params: dict[str, Any]
