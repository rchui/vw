"""Rendering infrastructure for SQL generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from strenum import StrEnum


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


@dataclass(kw_only=True, frozen=True)
class RenderResult:
    """Result of rendering a SQL statement."""

    sql: str
    params: dict[str, Any]
