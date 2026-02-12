"""PostgreSQL type descriptor functions for use with .cast().

Re-exports all core SQL types and adds PostgreSQL-specific types.

Example:
    col("id").cast(INTEGER())
    col("ts").cast(TIMESTAMPTZ())
    col("data").cast(JSONB())
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


def TIMESTAMPTZ() -> str:
    """PostgreSQL TIMESTAMPTZ type (timestamp with time zone)."""
    return "TIMESTAMPTZ"


def JSONB() -> str:
    """PostgreSQL JSONB type (binary JSON)."""
    return "JSONB"
