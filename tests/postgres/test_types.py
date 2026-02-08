"""Tests for vw/core/types.py and vw/postgres/types.py modules."""

import vw.core.types as core_types
import vw.postgres.types as pg_types


def describe_core_types() -> None:
    """Tests for vw.core.types functions."""

    def it_returns_smallint() -> None:
        assert core_types.SMALLINT() == "SMALLINT"

    def it_returns_integer() -> None:
        assert core_types.INTEGER() == "INTEGER"

    def it_returns_bigint() -> None:
        assert core_types.BIGINT() == "BIGINT"

    def it_returns_real() -> None:
        assert core_types.REAL() == "REAL"

    def it_returns_double_precision() -> None:
        assert core_types.DOUBLE_PRECISION() == "DOUBLE PRECISION"

    def it_returns_text() -> None:
        assert core_types.TEXT() == "TEXT"

    def it_returns_boolean() -> None:
        assert core_types.BOOLEAN() == "BOOLEAN"

    def it_returns_date() -> None:
        assert core_types.DATE() == "DATE"

    def it_returns_time() -> None:
        assert core_types.TIME() == "TIME"

    def it_returns_timestamp() -> None:
        assert core_types.TIMESTAMP() == "TIMESTAMP"

    def it_returns_json() -> None:
        assert core_types.JSON() == "JSON"

    def it_returns_uuid() -> None:
        assert core_types.UUID() == "UUID"

    def it_returns_bytea() -> None:
        assert core_types.BYTEA() == "BYTEA"

    def it_returns_varchar_without_length() -> None:
        assert core_types.VARCHAR() == "VARCHAR"

    def it_returns_varchar_with_length() -> None:
        assert core_types.VARCHAR(255) == "VARCHAR(255)"

    def it_returns_char_without_length() -> None:
        assert core_types.CHAR() == "CHAR"

    def it_returns_char_with_length() -> None:
        assert core_types.CHAR(3) == "CHAR(3)"

    def it_returns_numeric_without_params() -> None:
        assert core_types.NUMERIC() == "NUMERIC"

    def it_returns_numeric_with_precision() -> None:
        assert core_types.NUMERIC(10) == "NUMERIC(10)"

    def it_returns_numeric_with_precision_and_scale() -> None:
        assert core_types.NUMERIC(10, 2) == "NUMERIC(10,2)"

    def it_returns_decimal_without_params() -> None:
        assert core_types.DECIMAL() == "DECIMAL"

    def it_returns_decimal_with_precision() -> None:
        assert core_types.DECIMAL(10) == "DECIMAL(10)"

    def it_returns_decimal_with_precision_and_scale() -> None:
        assert core_types.DECIMAL(10, 2) == "DECIMAL(10,2)"


def describe_postgres_types() -> None:
    """Tests for vw.postgres.types functions."""

    def it_returns_timestamptz() -> None:
        assert pg_types.TIMESTAMPTZ() == "TIMESTAMPTZ"

    def it_returns_jsonb() -> None:
        assert pg_types.JSONB() == "JSONB"

    def it_re_exports_core_integer() -> None:
        assert pg_types.INTEGER() == "INTEGER"

    def it_re_exports_core_varchar() -> None:
        assert pg_types.VARCHAR(100) == "VARCHAR(100)"
