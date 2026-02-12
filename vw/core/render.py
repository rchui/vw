"""Shared rendering infrastructure for SQL generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strenum import StrEnum

from vw.core.exceptions import CTENameCollisionError

# --- Parameter Styles ------------------------------------------------------ #


class ParamStyle(StrEnum):
    """Supported parameter styles for rendering."""

    COLON = "colon"  # :param_name
    DOLLAR = "dollar"  # $param_name
    AT = "at"  # @param_name
    PYFORMAT = "pyformat"  # %(param_name)s


# --- Rendering Configuration ----------------------------------------------- #


@dataclass(kw_only=True, frozen=True)
class RenderConfig:
    """Configuration for SQL rendering."""

    param_style: ParamStyle


@dataclass(frozen=True, kw_only=True)
class RegisteredCTE:
    """A registered Common Table Expression for WITH clause."""

    name: str
    body_sql: str
    recursive: bool


@dataclass(kw_only=True)
class RenderContext:
    """Context for rendering SQL with parameter tracking and CTE registration."""

    config: RenderConfig
    params: dict[str, Any] = field(default_factory=dict)
    ctes: list[RegisteredCTE] = field(default_factory=list)

    def add_param(self, name: str, value: Any) -> str:
        """Add a parameter to the context and return its placeholder.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            The SQL placeholder string for this parameter.
        """
        self.params[name] = value

        param_style = self.config.param_style

        if param_style == ParamStyle.DOLLAR:
            return f"${name}"
        elif param_style == ParamStyle.AT:
            return f"@{name}"
        elif param_style == ParamStyle.COLON:
            return f":{name}"
        elif param_style == ParamStyle.PYFORMAT:
            return f"%({name})s"
        else:
            raise ValueError(f"Unsupported parameter style: {param_style}")

    def register_cte(self, name: str, body_sql: str, recursive: bool) -> None:
        """Register a CTE with its rendered body SQL.

        Args:
            name: CTE name
            body_sql: Rendered SQL for CTE body
            recursive: Whether this is a recursive CTE

        Raises:
            CTENameCollisionError: If CTE name already registered
        """

        for registered in self.ctes:
            if registered.name == name:
                raise CTENameCollisionError(f"CTE name '{name}' is already defined")

        self.ctes.append(RegisteredCTE(name=name, body_sql=body_sql, recursive=recursive))


# --- Rendering Result ------------------------------------------------------ #


@dataclass(kw_only=True, frozen=True)
class SQL:
    """Result of rendering a SQL statement."""

    query: str
    params: dict[str, Any]
