"""Snowflake type descriptor functions for use with .cast().

Re-exports all core SQL types and adds Snowflake-specific types.

Example:
    col("id").cast(INTEGER())
    col("data").cast(VARIANT())
    col("ts").cast(TIMESTAMP_NTZ())
"""

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


# Snowflake Semi-Structured Types
def VARIANT() -> str:
    """Snowflake VARIANT type (semi-structured data — JSON, Avro, ORC, Parquet, XML)."""
    return "VARIANT"


def OBJECT() -> str:
    """Snowflake OBJECT type (key-value pairs, similar to JSON object)."""
    return "OBJECT"


def ARRAY() -> str:
    """Snowflake ARRAY type (ordered sequence of values)."""
    return "ARRAY"


# Snowflake Numeric Type
def NUMBER(precision: int, scale: int = 0) -> str:
    """Snowflake NUMBER type with precision and scale.

    Args:
        precision: Total number of significant digits.
        scale: Number of digits to the right of the decimal point.

    Example:
        col("price").cast(NUMBER(10, 2))
        # Renders: CAST(price AS NUMBER(10, 2))
    """
    return f"NUMBER({precision}, {scale})"


# Snowflake Timestamp Types
def TIMESTAMP_NTZ() -> str:
    """Snowflake TIMESTAMP_NTZ type (timestamp without time zone)."""
    return "TIMESTAMP_NTZ"


def TIMESTAMP_LTZ() -> str:
    """Snowflake TIMESTAMP_LTZ type (timestamp with local time zone)."""
    return "TIMESTAMP_LTZ"


def TIMESTAMP_TZ() -> str:
    """Snowflake TIMESTAMP_TZ type (timestamp with time zone offset stored)."""
    return "TIMESTAMP_TZ"

