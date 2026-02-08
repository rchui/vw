"""PostgreSQL type descriptor functions for use with .cast().

Re-exports all core SQL types and adds PostgreSQL-specific types.

Example:
    col("id").cast(INTEGER())
    col("ts").cast(TIMESTAMPTZ())
    col("data").cast(JSONB())
"""

from vw.core.types import (
    BIGINT,
    BOOLEAN,
    BYTEA,
    CHAR,
    DATE,
    DECIMAL,
    DOUBLE_PRECISION,
    INTEGER,
    JSON,
    NUMERIC,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    UUID,
    VARCHAR,
)


def TIMESTAMPTZ() -> str:
    """PostgreSQL TIMESTAMPTZ type (timestamp with time zone)."""
    return "TIMESTAMPTZ"


def JSONB() -> str:
    """PostgreSQL JSONB type (binary JSON)."""
    return "JSONB"


__all__ = [
    "BIGINT",
    "BOOLEAN",
    "BYTEA",
    "CHAR",
    "DATE",
    "DECIMAL",
    "DOUBLE_PRECISION",
    "INTEGER",
    "JSON",
    "JSONB",
    "NUMERIC",
    "REAL",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "UUID",
    "VARCHAR",
]
