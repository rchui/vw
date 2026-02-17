"""DuckDB type descriptor functions for use with .cast()."""

from vw.core.types import BIGINT as BIGINT
from vw.core.types import BOOLEAN as BOOLEAN
from vw.core.types import BYTEA as BYTEA
from vw.core.types import CHAR as CHAR
from vw.core.types import DATE as DATE
from vw.core.types import DECIMAL as DECIMAL
from vw.core.types import DOUBLE_PRECISION as DOUBLE_PRECISION
from vw.core.types import INTEGER as INTEGER
from vw.core.types import JSON as JSON
from vw.core.types import NUMERIC as NUMERIC
from vw.core.types import REAL as REAL
from vw.core.types import SMALLINT as SMALLINT
from vw.core.types import TEXT as TEXT
from vw.core.types import TIME as TIME
from vw.core.types import TIMESTAMP as TIMESTAMP
from vw.core.types import UUID as UUID
from vw.core.types import VARCHAR as VARCHAR


# DuckDB Integer Variants
def TINYINT() -> str:
    """DuckDB TINYINT type (8-bit signed integer)."""
    return "TINYINT"


def UTINYINT() -> str:
    """DuckDB UTINYINT type (8-bit unsigned integer)."""
    return "UTINYINT"


def USMALLINT() -> str:
    """DuckDB USMALLINT type (16-bit unsigned integer)."""
    return "USMALLINT"


def UINTEGER() -> str:
    """DuckDB UINTEGER type (32-bit unsigned integer)."""
    return "UINTEGER"


def UBIGINT() -> str:
    """DuckDB UBIGINT type (64-bit unsigned integer)."""
    return "UBIGINT"


def HUGEINT() -> str:
    """DuckDB HUGEINT type (128-bit signed integer)."""
    return "HUGEINT"


def UHUGEINT() -> str:
    """DuckDB UHUGEINT type (128-bit unsigned integer)."""
    return "UHUGEINT"


# DuckDB Simple Types
def FLOAT() -> str:
    """DuckDB FLOAT type (alias for REAL)."""
    return "FLOAT"


def DOUBLE() -> str:
    """DuckDB DOUBLE type (alias for DOUBLE PRECISION)."""
    return "DOUBLE"


def BLOB() -> str:
    """DuckDB BLOB type (binary large object)."""
    return "BLOB"


def BIT() -> str:
    """DuckDB BIT type (single bit)."""
    return "BIT"


def INTERVAL() -> str:
    """DuckDB INTERVAL type (time interval)."""
    return "INTERVAL"


# DuckDB Parameterized Types
def LIST(element_type: str) -> str:
    """DuckDB LIST type (variable-length array).

    Args:
        element_type: Type string for list elements.

    Returns:
        DuckDB LIST type string.

    Example:
        col("tags").cast(LIST(VARCHAR()))
        # Renders: tags::LIST<VARCHAR>

        col("matrix").cast(LIST(LIST(INTEGER())))
        # Renders: matrix::LIST<LIST<INTEGER>>
    """
    return f"LIST<{element_type}>"


def STRUCT(fields: dict[str, str]) -> str:
    """DuckDB STRUCT type (named record type).

    Args:
        fields: Dictionary mapping field names to type strings.

    Returns:
        DuckDB STRUCT type string.

    Raises:
        ValueError: If fields dictionary is empty.

    Example:
        col("address").cast(STRUCT({"street": VARCHAR(), "city": VARCHAR(), "zip": INTEGER()}))
        # Renders: address::STRUCT<street: VARCHAR, city: VARCHAR, zip: INTEGER>
    """
    field_specs = ", ".join(f"{name}: {dtype}" for name, dtype in fields.items())
    return f"STRUCT<{field_specs}>"


def MAP(key_type: str, value_type: str) -> str:
    """DuckDB MAP type (key-value pairs).

    Args:
        key_type: Type string for map keys.
        value_type: Type string for map values.

    Returns:
        DuckDB MAP type string.

    Example:
        col("metadata").cast(MAP(VARCHAR(), VARCHAR()))
        # Renders: metadata::MAP<VARCHAR, VARCHAR>
    """
    return f"MAP<{key_type}, {value_type}>"


def ARRAY(element_type: str, size: int | None = None) -> str:
    """DuckDB ARRAY type (fixed-length array).

    DuckDB distinguishes between LIST (variable-length) and ARRAY (fixed-length).

    Args:
        element_type: Type string for array elements.
        size: Optional fixed size for array.

    Returns:
        DuckDB ARRAY type string.

    Example:
        col("coords").cast(ARRAY(DOUBLE(), 3))
        # Renders: coords::DOUBLE[3]

        col("values").cast(ARRAY(INTEGER()))
        # Renders: values::INTEGER[]
    """
    if size is not None:
        return f"{element_type}[{size}]"
    return f"{element_type}[]"
