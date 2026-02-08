# Research: Type Casting (Phase 7)

## Current State

### Already Implemented
- `Cast` dataclass in `vw/core/states.py:273-278` - `Cast(expr: Expr, data_type: str)`
- `.cast(data_type: str)` method on `Expression` in `vw/core/base.py:178-182`
- PostgreSQL rendering in `vw/postgres/render.py:153-155` - renders as `expr::type`
- Unit test in `tests/postgres/test_render.py:183-184`
- Integration test in `tests/postgres/integration/test_operators.py:335-344`

### Not Yet Implemented (per roadmap Phase 7)
- Type constructors (`VARCHAR`, `INTEGER`, `TIMESTAMP`, etc.)
- Dialect-specific type mapping
- `Cast` dataclass (exists but roadmap marks incomplete — needs roadmap update)
- Type system (`dtype` module — roadmap calls it that, but `types.py` is more Pythonic)

## Roadmap Items for Type Casting
From `docs/development/postgres-parity.md` lines 310-320:
```
### Type Casting
- [ ] CAST via `col("x").cast(dtype)`
- [ ] Type constructors (VARCHAR, INTEGER, TIMESTAMP, etc.)
- [ ] Dialect-specific type mapping

### Data Structures Needed
- [x] TextAccessor class for .text property
- [x] DateTimeAccessor class for .dt property
- [x] Interval dataclass (PostgreSQL-specific)
- [ ] Cast dataclass
- [ ] Type system (dtype module)
```

## Proposed Design

### Type System (`vw/core/types.py`)
A module of SQL data type descriptors that:
1. Can be passed to `.cast()` instead of a raw string
2. Render to SQL type strings (dialect-aware in the future)
3. Support parameterized types like `VARCHAR(255)`, `NUMERIC(10, 2)`

```python
# Simple usage
col("id").cast(types.INTEGER)           # → id::INTEGER
col("name").cast(types.VARCHAR(255))    # → name::VARCHAR(255)
col("price").cast(types.NUMERIC(10, 2)) # → price::NUMERIC(10,2)
col("ts").cast(types.TIMESTAMP)         # → ts::TIMESTAMP
```

### Type Classes

**Simple (no parameters):**
- `SMALLINT`, `INTEGER`, `BIGINT`
- `REAL`, `DOUBLE_PRECISION`
- `TEXT`, `BOOLEAN`
- `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`
- `JSON`, `JSONB` (PostgreSQL-specific)
- `UUID`
- `BYTEA`

**Parameterized (callable constructors):**
- `VARCHAR(n)` → `VARCHAR(n)` or `VARCHAR` if n is None
- `CHAR(n)` → `CHAR(n)`
- `NUMERIC(precision, scale)` → `NUMERIC(p,s)` or `NUMERIC(p)` or `NUMERIC`
- `DECIMAL(precision, scale)` → same as NUMERIC
- `TIMESTAMP(p)` → `TIMESTAMP(p)`
- `TIME(p)` → `TIME(p)`

### DataType Class Hierarchy
```python
@dataclass(frozen=True)
class DataType:
    name: str
    def __str__(self) -> str: return self.name

@dataclass(frozen=True)
class VarChar(DataType):
    length: int | None = None
    def __str__(self) -> str:
        return f"VARCHAR({self.length})" if self.length else "VARCHAR"

# etc.
```

### Cast State Update
Change `Cast.data_type` from `str` to `str | DataType`:
```python
@dataclass(eq=False, frozen=True, kw_only=True)
class Cast(Expr):
    expr: Expr
    data_type: str | DataType
```

### Render Update
The renderer converts `data_type` to string:
```python
case Cast():
    dtype_str = str(state.data_type)
    return f"{render_state(state.expr, ctx)}::{dtype_str}"
```
Since `str(DataType(...))` returns the SQL string and `str("INTEGER")` returns `"INTEGER"`, this works uniformly.

### Dialect-Specific Type Mapping
For Phase 1, types will use standard SQL names. Dialect mapping is prepared via a
`DataType.sql(dialect)` method for future expansion:
```python
class DataType:
    name: str
    def sql(self, dialect: str = "ansi") -> str: return self.name
```

Future: PostgreSQL-specific types (`JSONB`, `BYTEA`, `UUID`) will live in `vw/postgres/types.py`.

## Architecture Pattern
Follows the existing pattern for grouped functionality:
- `vw/core/text.py` → `TextAccessor` for `.text` accessor
- `vw/core/datetime.py` → `DateTimeAccessor` for `.dt` accessor
- `vw/core/types.py` → type constants/constructors for `.cast()`

## Files to Create/Modify
| File | Action | Purpose |
|------|--------|---------|
| `vw/core/types.py` | Create | Type system module |
| `vw/core/states.py` | Modify | `Cast.data_type: str \| DataType` |
| `vw/core/base.py` | Modify | `.cast()` type signature |
| `vw/postgres/render.py` | Modify | Use `str(state.data_type)` |
| `tests/core/test_types.py` | Create | Unit tests for types module |
| `tests/postgres/test_render.py` | Modify | Update cast test, add type tests |
| `tests/postgres/integration/test_operators.py` | Modify | Add type constructor tests |
| `docs/development/postgres-parity.md` | Modify | Mark items complete |
| `docs/api/core.md` | Modify | Document types module |
