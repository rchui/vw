# Research: Phase 7 Date/Time Functions

## Scope

Core gets ANSI SQL constructs only. Postgres extends with dialect-specific features.

## Architecture

### Core (`vw/core`)
- `Extract` state: `EXTRACT(field FROM expr)` — ANSI SQL
- `CurrentTimestamp`, `CurrentDate`, `CurrentTime` states — ANSI SQL
- `DateTimeAccessor` class with `.extract(field: str)`
- `F.current_timestamp()`, `F.current_date()`, `F.current_time()`
- `Expression.dt` property returns core `DateTimeAccessor`

### PostgreSQL (`vw/postgres`)
- `PostgresDateTimeAccessor(DateTimeAccessor)` — subclass for postgres-specific additions (e.g. future `date_trunc`)
- PostgreSQL `Expression` overrides `.dt` to return `PostgresDateTimeAccessor`
- `Now` state: `NOW()` — postgres-specific
- `Interval` state: `INTERVAL 'amount unit'` — postgres-specific syntax
- `F.now()` on postgres `Functions`
- `interval(value, unit)` top-level factory
- Interval arithmetic uses existing `+`/`-` `Operator` state (no extra work)

## Deferred
- `DATE_TRUNC` — postgres/duckdb specific, future `PostgresDateTimeAccessor` method
- Individual `.year()`, `.month()` etc. — replaced by `.extract(field)`

## How the `.text` Pattern Works (reference for `.dt`)

- Core defines `Expression.text` returning `TextAccessor[ExprT, RowSetT]` (lazy import)
- Postgres has empty `Expression(CoreExpression)` subclass in `vw/postgres/base.py`
- Postgres factories (`Factories(expr=Expression, rowset=RowSet)`) ensure all operations return postgres types
- For postgres-specific accessor methods, the postgres `Expression` overrides the property to return the dialect subclass

## Key Design Decisions

1. `dt.extract(field: str)` — single method, flexible, field validated at DB level
2. Separate state types for special syntax (EXTRACT has `EXTRACT(field FROM expr)`, CURRENT_TIMESTAMP has no parens)
3. `PostgresDateTimeAccessor` extends core accessor; postgres `Expression` overrides `.dt`
4. `interval()` is a top-level factory (like `param()`, `col()`), not on `F`

## Files to Modify/Create

| File | Change |
|------|--------|
| `vw/core/states.py` | Add `Extract`, `CurrentTimestamp`, `CurrentDate`, `CurrentTime` |
| `vw/core/datetime.py` | New `DateTimeAccessor` with `.extract()` |
| `vw/core/base.py` | Add `.dt` property to `Expression` |
| `vw/core/functions.py` | Add `current_timestamp`, `current_date`, `current_time` to `Functions` |
| `vw/postgres/base.py` | Override `.dt` on postgres `Expression` to return `PostgresDateTimeAccessor` |
| `vw/postgres/datetime.py` | New `PostgresDateTimeAccessor(DateTimeAccessor)` |
| `vw/postgres/render.py` | Add rendering for all new states |
| `vw/postgres/functions.py` | Add `now()` to postgres `Functions` (currently in public.py) |
| `vw/postgres/public.py` | Add `interval()` factory, add `Now`/`Interval` states |
| `vw/postgres/__init__.py` | Export `interval` |
| `tests/postgres/integration/test_datetime_functions.py` | Full test suite |
