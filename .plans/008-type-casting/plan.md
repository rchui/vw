# Plan: Type Casting (Phase 7 Completion)

## Goal
Complete Phase 7 type casting by adding a types module where types are plain
functions returning SQL type strings. Update `.cast()` to document this usage.
Core types live in `vw.core.types`, and dialect-specific types live in
`vw.postgres.types` (which re-exports core types plus adds its own).

## Scope
- New `vw/core/types.py` module — functions returning SQL type strings
- New `vw/postgres/types.py` module — re-exports core types + postgres-specific
- No changes to `Cast` state or `.cast()` — they already accept strings
- Tests and documentation

## Non-Goals
- DuckDB types (DuckDB render.py doesn't exist yet)
- Type validation or dialect-specific type name aliasing — future work

## Implementation Steps

### Step 1: Create `vw/core/types.py`
Each type is a plain function returning a string. Parameterized types accept
optional arguments.

```python
# Simple types (no params)
def smallint() -> str: return "smallint"
def integer() -> str: return "integer"
def bigint() -> str: return "bigint"
def real() -> str: return "real"
def double_precision() -> str: return "double precision"
def text() -> str: return "text"
def boolean() -> str: return "boolean"
def date() -> str: return "date"
def time() -> str: return "time"
def timestamp() -> str: return "timestamp"
def json() -> str: return "json"
def uuid() -> str: return "uuid"
def bytea() -> str: return "bytea"

# Parameterized types
def varchar(length: int | None = None) -> str:
    return f"varchar({length})" if length else "varchar"

def char(length: int | None = None) -> str:
    return f"char({length})" if length else "char"

def numeric(precision: int | None = None, scale: int | None = None) -> str:
    if precision and scale: return f"numeric({precision},{scale})"
    if precision: return f"numeric({precision})"
    return "numeric"

def decimal(precision: int | None = None, scale: int | None = None) -> str:
    if precision and scale: return f"decimal({precision},{scale})"
    if precision: return f"decimal({precision})"
    return "decimal"
```

### Step 2: Create `vw/postgres/types.py`
Re-export all core types and add PostgreSQL-specific types.

```python
from vw.core.types import (
    smallint, integer, bigint, real, double_precision,
    text, boolean, date, time, timestamp, json, uuid, bytea,
    varchar, char, numeric, decimal,
)

def timestamptz() -> str: return "timestamptz"
def jsonb() -> str: return "jsonb"
```

### Step 3: Add tests
- `tests/core/test_types.py` — test each type function returns correct string
- `tests/postgres/integration/test_operators.py` — end-to-end with types

### Step 4: Update roadmap and docs
- Mark Phase 7 type casting items as complete in `docs/development/postgres-parity.md`
- Document types in `docs/api/core.md` and `docs/api/postgres.md`

## API Design
```python
from vw.postgres import types

col("id").cast(types.integer())
col("name").cast(types.varchar(255))
col("price").cast(types.numeric(10, 2))
col("ts").cast(types.timestamptz())

# Raw strings still work
col("id").cast("text")
```
