"""SQL type descriptor functions for use with .cast().

Each function returns the SQL type string for the given type.
Parameterized types accept optional arguments.

Example:
    col("id").cast(INTEGER())
    col("name").cast(VARCHAR(255))
    col("price").cast(NUMERIC(10, 2))
"""


def SMALLINT() -> str:
    """SQL SMALLINT type."""
    return "SMALLINT"


def INTEGER() -> str:
    """SQL INTEGER type."""
    return "INTEGER"


def BIGINT() -> str:
    """SQL BIGINT type."""
    return "BIGINT"


def REAL() -> str:
    """SQL REAL type."""
    return "REAL"


def DOUBLE_PRECISION() -> str:
    """SQL DOUBLE PRECISION type."""
    return "DOUBLE PRECISION"


def TEXT() -> str:
    """SQL TEXT type."""
    return "TEXT"


def BOOLEAN() -> str:
    """SQL BOOLEAN type."""
    return "BOOLEAN"


def DATE() -> str:
    """SQL DATE type."""
    return "DATE"


def TIME() -> str:
    """SQL TIME type."""
    return "TIME"


def TIMESTAMP() -> str:
    """SQL TIMESTAMP type."""
    return "TIMESTAMP"


def JSON() -> str:
    """SQL JSON type."""
    return "JSON"


def UUID() -> str:
    """SQL UUID type."""
    return "UUID"


def BYTEA() -> str:
    """SQL BYTEA type."""
    return "BYTEA"


def VARCHAR(length: int | None = None) -> str:
    """SQL VARCHAR type, optionally with length."""
    if length is not None:
        return f"VARCHAR({length})"
    return "VARCHAR"


def CHAR(length: int | None = None) -> str:
    """SQL CHAR type, optionally with length."""
    if length is not None:
        return f"CHAR({length})"
    return "CHAR"


def NUMERIC(precision: int | None = None, scale: int | None = None) -> str:
    """SQL NUMERIC type, optionally with precision and scale."""
    if precision is not None and scale is not None:
        return f"NUMERIC({precision},{scale})"
    if precision is not None:
        return f"NUMERIC({precision})"
    return "NUMERIC"


def DECIMAL(precision: int | None = None, scale: int | None = None) -> str:
    """SQL DECIMAL type, optionally with precision and scale."""
    if precision is not None and scale is not None:
        return f"DECIMAL({precision},{scale})"
    if precision is not None:
        return f"DECIMAL({precision})"
    return "DECIMAL"
