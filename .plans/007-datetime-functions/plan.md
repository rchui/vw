# Plan: Phase 7 Date/Time Functions

## Goal

Implement ANSI SQL date/time support in core and PostgreSQL-specific extensions in the postgres dialect.

## New States

### Core (`vw/core/states.py`)

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Extract(Expr):
    """EXTRACT(field FROM expr) — ANSI SQL."""
    field: str
    expr: Expr


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentTimestamp(Expr):
    """CURRENT_TIMESTAMP — ANSI SQL."""


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentDate(Expr):
    """CURRENT_DATE — ANSI SQL."""


@dataclass(eq=False, frozen=True, kw_only=True)
class CurrentTime(Expr):
    """CURRENT_TIME — ANSI SQL."""
```

### PostgreSQL (inline in `vw/postgres/render.py` or new `vw/postgres/states.py`)

```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Now(Expr):
    """NOW() — PostgreSQL-specific."""


@dataclass(eq=False, frozen=True, kw_only=True)
class Interval(Expr):
    """INTERVAL 'amount unit' — PostgreSQL-specific syntax."""
    amount: int | float
    unit: str
```

## New Files

### `vw/core/datetime.py`

```python
class DateTimeAccessor(Generic[ExprT, RowSetT]):
    def __init__(self, expr: Expression[ExprT, RowSetT]) -> None:
        self.expr = expr

    def extract(self, field: str) -> ExprT:
        """EXTRACT(field FROM expr)"""
        state = Extract(field=field, expr=self.expr.state)
        return self.expr.factories.expr(state=state, factories=self.expr.factories)
```

### `vw/postgres/datetime.py`

```python
class PostgresDateTimeAccessor(DateTimeAccessor[ExprT, RowSetT]):
    """PostgreSQL date/time accessor. Extends core with postgres-specific functions."""
    # Placeholder for future date_trunc, etc.
    pass
```

## Modified Files

### `vw/core/base.py`
Add `.dt` property to `Expression`:
```python
@property
def dt(self) -> DateTimeAccessor[ExprT, RowSetT]:
    from vw.core.datetime import DateTimeAccessor
    return DateTimeAccessor(self)
```

### `vw/postgres/base.py`
Override `.dt` on postgres `Expression`:
```python
@property
def dt(self) -> PostgresDateTimeAccessor[ExprT, RowSetT]:
    from vw.postgres.datetime import PostgresDateTimeAccessor
    return PostgresDateTimeAccessor(self)
```

### `vw/core/functions.py`
Add to `Functions` class:
```python
def current_timestamp(self) -> ExprT: ...
def current_date(self) -> ExprT: ...
def current_time(self) -> ExprT: ...
```

### `vw/postgres/public.py` (postgres Functions subclass)
Add `now()` and `interval()` factory.

### `vw/postgres/render.py`
Add rendering for: `Extract`, `CurrentTimestamp`, `CurrentDate`, `CurrentTime`, `Now`, `Interval`.

## Rendering

| State | SQL Output |
|-------|-----------|
| `Extract(field="year", expr=...)` | `EXTRACT(YEAR FROM ...)` |
| `CurrentTimestamp()` | `CURRENT_TIMESTAMP` |
| `CurrentDate()` | `CURRENT_DATE` |
| `CurrentTime()` | `CURRENT_TIME` |
| `Now()` | `NOW()` |
| `Interval(amount=1, unit="day")` | `INTERVAL '1 day'` |

Fields in EXTRACT are uppercased during rendering.
