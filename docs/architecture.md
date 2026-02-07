# Architecture

vw is designed with a clean separation of concerns: a dialect-agnostic core layer and dialect-specific implementations.

## Component Overview

```
┌──────────────────────────────────────────────────────────┐
│                    User Code                             │
│  from vw.postgres as vw, col, param, render              │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ↓
        ┌────────────────────────────────────────────┐
        │     vw/postgres (Dialect-Specific)         │
        │  - Factory functions (source, col, param)  │
        │  - Public API exports                       │
        │  - PostgreSQL-specific rendering            │
        │  - Parameter style: $name                   │
        └────────────────────┬───────────────────────┘
                             │
                             ↓ (inherits)
        ┌────────────────────────────────────────────┐
        │        vw/core (Abstract Layer)            │
        │  - Expression (wrapper class)              │
        │  - RowSet (wrapper class)                  │
        │  - State dataclasses (Reference,           │
        │    SetOperation, Statement, etc.)          │
        │  - Functions (aggregate/window)            │
        │  - Rendering infrastructure                │
        └────────────────────────────────────────────┘
```

## vw/core - Abstract Core

The core layer provides dialect-agnostic abstractions and shared infrastructure.

### Key Components

#### 1. States (vw/core/states.py)

Immutable dataclasses representing SQL concepts:

**Sources:**
- **Reference** - Table or view reference (inherits from Source)
- **SetOperation** - UNION/INTERSECT/EXCEPT operations (inherits from Source)

**Queries:**
- **Statement** - SELECT query with all clauses

**Expressions:**
- **Column** - Column reference (qualified or unqualified)
- **Parameter** - Query parameter with name and value
- **Operators** - Equals, LessThan, Add, And, etc.
- **Functions** - Function calls (aggregate/window)
- **Window** - Window function with OVER clause

**Modifiers:**
- **Distinct** - DISTINCT flag
- **Limit** - LIMIT and OFFSET

All states are frozen dataclasses - they cannot be modified after creation.

#### 2. Wrappers (vw/core/base.py)

Wrapper classes that provide fluent APIs:

**Expression**
- Wraps any expression state
- Provides operator overloads (`==`, `<`, `>`, `+`, `-`, etc.)
- Provides methods (`.like()`, `.is_null()`, `.cast()`, etc.)
- Returns new Expression instances

**RowSet**
- Wraps Reference, Statement, or SetOperation
- Provides query building methods (`.select()`, `.where()`, `.group_by()`, etc.)
- Returns new RowSet instances
- Transforms Reference or SetOperation → Statement on first query method
- Provides set operation operators (`|`, `+`, `&`, `-`) that return RowSet wrapping SetOperation

#### 3. Functions (vw/core/functions.py)

Factory class for aggregate and window functions:
- COUNT, SUM, AVG, MIN, MAX
- ROW_NUMBER, RANK, DENSE_RANK, NTILE
- LAG, LEAD, FIRST_VALUE, LAST_VALUE

#### 4. Rendering (vw/core/render.py)

Base infrastructure for SQL rendering:
- **ParamStyle** - Enum for parameter styles (COLON, DOLLAR, AT, PYFORMAT)
- **RenderConfig** - Configuration for rendering
- **RenderContext** - Tracks parameters during rendering
- **SQL** - Result dataclass with query and params

## vw/postgres - PostgreSQL Dialect

The PostgreSQL layer provides concrete implementations for PostgreSQL.

### Structure

```
vw/postgres/
├── __init__.py      → Public exports
├── base.py          → PostgreSQL-specific Expression/RowSet classes
├── public.py        → Factory functions (source, col, param, render)
└── render.py        → PostgreSQL SQL rendering logic
```

### Key Components

#### 1. Factory Functions (vw/postgres/public.py)

```python
def source(name: str) -> RowSet:
    """Create a PostgreSQL table/view source"""

def col(name: str) -> Expression:
    """Create a PostgreSQL column reference"""

def param(name: str, value: Any) -> Expression:
    """Create a PostgreSQL parameter"""

def render(rowset: RowSet) -> SQL:
    """Render to PostgreSQL SQL"""
```

#### 2. Rendering (vw/postgres/render.py)

Converts state objects to PostgreSQL SQL:
- Uses `$name` parameter style
- Handles PostgreSQL-specific syntax
- Collects parameters into a dictionary
- Implements proper SQL clause ordering

Example rendering chain:
```
Statement(
    source=Source(name="users"),
    columns=[Column(name="id")],
    where=[Equals(left=Column(name="age"), right=Parameter(name="age", value=18))]
)
    ↓ (render)
SQL(
    query="SELECT id FROM users WHERE age = $age",
    params={"age": 18}
)
```

## vw/duckdb - DuckDB Dialect (Incomplete)

The DuckDB layer will provide DuckDB-specific implementations.

### Status

Currently minimal with basic structure:
- `base.py` - Placeholder classes
- `public.py` - Basic source factory
- Missing: render.py, full API

### Planned Features

- DuckDB parameter style
- Star extensions (EXCLUDE, REPLACE)
- File I/O (read_csv, read_parquet, COPY)
- DuckDB-specific functions

## Design Patterns

### 1. Immutability

All state objects are frozen dataclasses. Query building never mutates - it always creates new objects:

```python
users = source("users")
q1 = users.select(col("id"))
q2 = q1.where(col("age") > 18)
# users, q1 are unchanged
```

### 2. State Wrapping

States are simple data containers. Wrappers (Expression, RowSet) provide the API:

```python
# State: simple dataclass
column_state = Column(name="age")

# Wrapper: provides API
column = Expression(state=column_state)
result = column >= 18  # Returns new Expression wrapping Equals state
```

### 3. Lazy Rendering

Queries are not converted to SQL until `render()` is called:

```python
query = source("users").select(col("id"))  # No SQL yet
result = render(query)  # Now converted to SQL
```

### 4. Type Safety

Factory functions return typed objects:

```python
from vw.postgres as vw, col, param

users: RowSet = source("users")
age: Expression = col("age")
min_age: Expression = param("min_age", 18)
```

## Query Building Flow

1. **User imports dialect-specific API**
   ```python
   from vw.postgres as vw, col, param, render
   ```

2. **User builds query with fluent API**
   ```python
   query = (
       source("users")
       .select(col("id"), col("name"))
       .where(col("age") >= param("min_age", 18))
   )
   ```

3. **Under the hood, immutable states are created**
   ```
   RowSet(state=Statement(
       source=Source(name="users"),
       columns=[Column(name="id"), Column(name="name")],
       where=[Equals(
           left=Column(name="age"),
           right=Parameter(name="min_age", value=18)
       )]
   ))
   ```

4. **User renders to SQL**
   ```python
   result = render(query)
   ```

5. **Dialect-specific renderer walks the state tree**
   ```
   Statement → SELECT ... FROM ... WHERE ...
   Column → "id", "name"
   Equals → age = $min_age
   Parameter → collected into params dict
   ```

6. **Result is SQL + params**
   ```python
   SQL(
       query="SELECT id, name FROM users WHERE age >= $min_age",
       params={"min_age": 18}
   )
   ```

## Extension Points

### Adding a New Dialect

1. Create `vw/mydialect/` directory
2. Create `base.py` with dialect-specific Expression/RowSet
3. Create `public.py` with factory functions
4. Create `render.py` with SQL rendering logic
5. Create `__init__.py` with exports
6. Reuse vw/core completely

### Adding a New Feature

1. Add state dataclass to `vw/core/states.py`
2. Add method to wrapper class in `vw/core/base.py`
3. Add rendering logic to `vw/postgres/render.py`
4. Add tests
5. DuckDB automatically gets the structure

## Testing Philosophy

- **Unit tests** - Test individual methods and state creation
- **Integration tests** - Test query building and rendering together
- **Dialect tests** - Separate test suites for each dialect
- **Assertion style** - Assert entire SQL strings, not fragments

## Next Steps

- **[API Reference](api/index.md)** - Detailed API documentation
- **[PostgreSQL API](api/postgres.md)** - PostgreSQL-specific API
- **[DuckDB API](api/duckdb.md)** - DuckDB-specific API
