"""DuckDB file reading support.

This module provides support for reading files as row sources in DuckDB.
Currently supports CSV format with comprehensive options.

Example:
    >>> from vw.duckdb import file, csv, col
    >>> query = file("users.csv", csv(header=True, delim=",")).select(col("name"))
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from vw.core.base import Factories
from vw.core.states import File

if TYPE_CHECKING:
    from vw.duckdb.base import RowSet


@dataclass(eq=False, frozen=True, kw_only=True)
class CSV:
    """CSV format modifier for file reading.

    Represents CSV-specific options for reading CSV files in DuckDB.
    All options are optional and will use DuckDB's defaults if not specified.

    Attributes:
        header: Whether the file has a header row (default: auto-detect).
        delim: Delimiter character (default: auto-detect, usually ',').
        quote: Quote character (default: '"').
        escape: Escape character (default: '"').
        compression: Compression format ('none', 'gzip', 'zstd', default: auto-detect).
        columns: Dictionary mapping column names to types (default: auto-detect).
        all_varchar: Treat all columns as VARCHAR instead of type detection (default: False).
        dateformat: Date format string for parsing dates (default: ISO 8601).
        timestampformat: Timestamp format string for parsing timestamps (default: ISO 8601).
        skip: Number of rows to skip at the start of the file (default: 0).
        null_padding: Add NULL values for missing columns (default: False).
        sample_size: Number of rows to sample for auto-detection (default: 20480).
        ignore_errors: Ignore rows with parsing errors (default: False).
        auto_type_candidates: List of types to consider during auto-detection.
        parallel: Enable parallel CSV reading (default: True).
        filename: Add filename column to output (default: False).
        hive_partitioning: Enable Hive partitioning (default: False).
        union_by_name: Union by column name instead of position (default: False).
        max_line_size: Maximum line size in bytes (default: 2097152).
        names: List of column names (overrides header row).
        types: List of column types (parallel to names or header).
        decimal_separator: Decimal separator character (default: '.').

    Example:
        >>> CSV(header=True, delim=",")
        >>> CSV(all_varchar=True, skip=10)
        >>> CSV(dateformat="%Y-%m-%d", ignore_errors=True)
    """

    header: bool | None = None
    delim: str | None = None
    quote: str | None = None
    escape: str | None = None
    compression: str | None = None
    columns: dict[str, str] | None = None
    all_varchar: bool | None = None
    dateformat: str | None = None
    timestampformat: str | None = None
    skip: int | None = None
    null_padding: bool | None = None
    sample_size: int | None = None
    ignore_errors: bool | None = None
    auto_type_candidates: list[str] | None = None
    parallel: bool | None = None
    filename: bool | None = None
    hive_partitioning: bool | None = None
    union_by_name: bool | None = None
    max_line_size: int | None = None
    names: list[str] | None = None
    types: list[str] | None = None
    decimal_separator: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class Parquet:
    """Parquet format modifier for file() function.

    All options correspond to DuckDB's read_parquet() function parameters.
    All fields are optional and will only be rendered if not None.

    Common options:
        binary_as_string: Read BINARY/VARBINARY columns as VARCHAR (default: False)
        filename: Add filename column to output (default: False)
        file_row_number: Add file_row_number column to output (default: False)
        hive_partitioning: Enable Hive partitioning (default: False)
        union_by_name: Union by column name instead of position (default: False)
        compression: Compression codec (e.g., 'snappy', 'gzip', 'zstd')
    """

    binary_as_string: bool | None = None
    filename: bool | None = None
    file_row_number: bool | None = None
    hive_partitioning: bool | None = None
    union_by_name: bool | None = None
    compression: str | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class JSON:
    """JSON format modifier for file() function (standard JSON format).

    All options correspond to DuckDB's read_json() function parameters.
    All fields are optional and will only be rendered if not None.

    Common options:
        columns: Dict mapping column names to SQL types
        maximum_object_size: Maximum size of JSON objects (default: 16777216)
        ignore_errors: Ignore parse errors (default: False)
        compression: Compression type ('none', 'gzip', 'zstd', etc.)
        filename: Add filename column to output (default: False)
        hive_partitioning: Enable Hive partitioning (default: False)
        union_by_name: Union by column name instead of position (default: False)
    """

    columns: dict[str, str] | None = None
    maximum_object_size: int | None = None
    ignore_errors: bool | None = None
    compression: str | None = None
    filename: bool | None = None
    hive_partitioning: bool | None = None
    union_by_name: bool | None = None


@dataclass(eq=False, frozen=True, kw_only=True)
class JSONL:
    """JSONL format modifier for file() function (newline-delimited JSON).

    Renders as read_json() with format='newline_delimited'.
    All options correspond to DuckDB's read_json() function parameters.
    All fields are optional and will only be rendered if not None.

    Common options:
        columns: Dict mapping column names to SQL types
        maximum_object_size: Maximum size of JSON objects (default: 16777216)
        ignore_errors: Ignore parse errors (default: False)
        compression: Compression type ('none', 'gzip', 'zstd', etc.)
        filename: Add filename column to output (default: False)
        hive_partitioning: Enable Hive partitioning (default: False)
        union_by_name: Union by column name instead of position (default: False)
    """

    columns: dict[str, str] | None = None
    maximum_object_size: int | None = None
    ignore_errors: bool | None = None
    compression: str | None = None
    filename: bool | None = None
    hive_partitioning: bool | None = None
    union_by_name: bool | None = None


def file(*paths: str, format: CSV | Parquet | JSON | JSONL) -> RowSet:
    """Create a file read operation as a row source.

    Read a file (or multiple files) using DuckDB's file reading capabilities.
    The file can be used anywhere a table can be used (FROM, JOIN, subqueries).

    Args:
        path: Single file path or list of file paths. Supports wildcards (*.csv).
        format: Format specifying file type and options (CSV, Parquet, JSON, JSONL).

    Returns:
        A RowSet wrapping a File state that can be queried like a table.

    Example:
        >>> from vw.duckdb import file, CSV, col
        >>> # Read single CSV file
        >>> query = file("users.csv", format=CSV(header=True)).select(col("name"))
        >>> # Read with custom options
        >>> query = file("data.csv", format=CSV(delim="|", skip=1, all_varchar=True))
        >>> # Read multiple files
        >>> query = file("data1.csv", "data2.csv", format=CSV(header=True))
        >>> # Read with wildcards
        >>> query = file("data/*.csv", format=CSV(header=True, union_by_name=True))
    """
    from vw.duckdb.base import Expression, RowSet

    return RowSet(
        state=File(paths=paths, format=format),
        factories=Factories(expr=Expression, rowset=RowSet),
    )
