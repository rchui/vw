"""Rendering infrastructure for SQL generation."""

from dataclasses import dataclass, field
from typing import Any

from strenum import StrEnum


class ParameterStyle(StrEnum):
    """Supported parameter styles for SQL rendering."""

    COLON = "colon"
    """Colon-prefixed named parameters: :name (SQLAlchemy, SQLite, Oracle)"""

    DOLLAR = "dollar"
    """Dollar-prefixed named parameters: $name"""

    AT = "at"
    """At-prefixed named parameters: @name (SQL Server)"""


@dataclass(kw_only=True, frozen=True)
class RenderConfig:
    """Configuration for SQL rendering."""

    parameter_style: ParameterStyle = ParameterStyle.COLON


@dataclass(kw_only=True)
class RenderContext:
    """Context for rendering SQL with parameter tracking."""

    config: RenderConfig
    params: dict[str, Any] = field(default_factory=dict)
    depth: int = 0

    def recurse(self) -> "RenderContext":
        """Create a child context for nested rendering."""
        return RenderContext(config=self.config, params=self.params, depth=self.depth + 1)

    def add_param(self, name: str, value: Any) -> str:
        """
        Add a parameter to the context and return its placeholder.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            The SQL placeholder string for this parameter.

        Raises:
            ValueError: If the parameter style is not supported.
        """
        self.params[name] = value
        if self.config.parameter_style == ParameterStyle.COLON:
            return f":{name}"
        elif self.config.parameter_style == ParameterStyle.DOLLAR:
            return f"${name}"
        elif self.config.parameter_style == ParameterStyle.AT:
            return f"@{name}"
        raise ValueError(f"Unsupported parameter style: {self.config.parameter_style}")


@dataclass(kw_only=True, frozen=True)
class RenderResult:
    """Result of rendering a SQL statement."""

    sql: str
    params: dict[str, Any]
