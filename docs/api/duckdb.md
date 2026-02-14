# DuckDB API Reference

The `vw.duckdb` module provides DuckDB-specific implementations and rendering.

**Status:** ✅ **Core Features Complete** - Basic DuckDB support is now available. DuckDB-specific features (star extensions, file I/O) are pending.

## Module Import

```python
from vw.duckdb import ref, col, param, lit, render, F
```

## Factory Functions

### `ref(name)`

Create a reference to a table or view.

```python
users = ref("users")
# Represents: users table
```

**Parameters:**
- `name` (str) - Table or view name

**Returns:** RowSet wrapping Reference state

**Status:** ✅ Available

### `col(name)`

Create a column reference.

```python
col("name")
col("users.id")
col("*")  # SELECT * syntax
```

**Parameters:**
- `name` (str) - Column name (can include table qualifier)

**Returns:** Expression wrapping Column state

**Status:** ✅ Available

### `param(name, value)`

Create a parameter for parameterized queries.

```python
param("min_age", 18)
ref("users").where(col("age") >= param("min_age", 18))
```

**Parameters:**
- `name` (str) - Parameter name (will be rendered as $name in DuckDB)
- `value` (Any) - Parameter value

**Returns:** Expression wrapping Parameter state

**Status:** ✅ Available

### `lit(value)`

Create a literal value (rendered directly in SQL).

```python
lit(42)
lit("hello")
lit(True)
lit(None)  # NULL
```

**Parameters:**
- `value` (Any) - The literal value (int, float, str, bool, None)

**Returns:** Expression wrapping Literal state

**Status:** ✅ Available

### `render(rowset)`

Render a RowSet or Expression to DuckDB SQL.

```python
query = ref("users").select(col("id"), col("name"))
result = render(query)
# result.query: "SELECT id, name FROM users"
# result.params: {}
```

**Parameters:**
- `rowset` (RowSet | Expression) - Query to render
- `config` (RenderConfig | None) - Optional rendering configuration

**Returns:** SQL result with query string and parameters dict

**Status:** ✅ Available

## Core Features

### Rendering
- ✅ `vw/duckdb/render.py` - DuckDB SQL rendering logic
- ✅ Parameter style configuration (DOLLAR: `$1`, `$2`)
- ✅ DuckDB-specific syntax handling
- ✅ Identifier quoting (double quotes)

### Functions
- ✅ `F` - Functions instance with all ANSI SQL standard functions
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- ✅ Date/time functions (CURRENT_TIMESTAMP, CURRENT_DATE, EXTRACT)
- ✅ String functions (UPPER, LOWER, TRIM, LENGTH, SUBSTRING, etc.)
- ✅ Null handling (COALESCE, NULLIF, GREATEST, LEAST)

### Query Building
- ✅ SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- ✅ LIMIT, OFFSET, FETCH, DISTINCT
- ✅ Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- ✅ Subqueries, CTEs, VALUES clause
- ✅ Set operations (UNION, INTERSECT, EXCEPT)
- ✅ CASE WHEN expressions
- ✅ Window functions with PARTITION BY, ORDER BY, frames

## Pending DuckDB-Specific Features

The following features are not yet implemented for DuckDB:

### DuckDB-Specific Features (Planned)

#### Star Extensions
- ❌ `SELECT * EXCLUDE (col1, col2)` - Exclude columns from star
- ❌ `SELECT * REPLACE (expr AS col)` - Replace column in star

#### File I/O
- ❌ `read_csv(path)` - Read CSV as source
- ❌ `read_parquet(path)` - Read Parquet as source
- ❌ `read_json(path)` - Read JSON as source
- ❌ `COPY FROM` - Copy from file to table
- ❌ `COPY TO` - Copy from table/query to file

#### DuckDB Functions
- ❌ List/array functions
- ❌ Struct functions
- ❌ DuckDB-specific aggregates
- ❌ DuckDB-specific window functions

#### DuckDB Types
- ❌ LIST type
- ❌ STRUCT type
- ❌ MAP type
- ❌ UNION type

## Development Status

DuckDB support is waiting for PostgreSQL implementation to stabilize. See [DuckDB Parity Roadmap](../development/duckdb-parity.md) for full details.

**Prerequisites:**
- ✅ PostgreSQL Phase 1: Core Query Building (complete)
- ✅ PostgreSQL Phase 2: Operators & Expressions (complete)
- ✅ PostgreSQL Phase 3: Aggregate & Window Functions (complete)
- ✅ PostgreSQL Phase 6: Parameters & Rendering (complete)
- 📋 PostgreSQL Phase 4: Joins (pending)

**Implementation Plan:**
1. Complete core PostgreSQL features (Phases 1-6)
2. Implement basic DuckDB rendering (parameter style, SQL generation)
3. Add DuckDB-specific features (star extensions, file I/O)
4. Add DuckDB-specific functions and types

**Estimated Completion:**
- Basic DuckDB support: After PostgreSQL reaches ~60% completion
- Full DuckDB support: After PostgreSQL reaches ~80% completion

## Example (Future API)

Once implemented, the DuckDB API will look like this:

```python
# This is illustrative - not yet implemented
from vw.duckdb import source, col, param, render, read_csv, F

# Read CSV as source
users = read_csv("users.csv")

# Build query
query = (
    users
    .select(col("*").exclude("password", "ssn"))  # Star with EXCLUDE
    .where(col("age") >= param("min_age", 18))
    .order_by(col("name").asc())
)

# Render
result = render(query)

# Execute with DuckDB
import duckdb
conn = duckdb.connect('mydb.duckdb')
rows = conn.execute(result.query, result.params).fetchall()
```

## Contributing

If you'd like to help implement DuckDB support:

1. Check the [DuckDB Parity Roadmap](../development/duckdb-parity.md)
2. Start with Phase 1: Core DuckDB Implementation
3. Follow the existing patterns from `vw/postgres`
4. Add tests following the PostgreSQL test patterns

## Next Steps

- **[PostgreSQL API](postgres.md)** - See the complete PostgreSQL API for reference
- **[DuckDB Parity](../development/duckdb-parity.md)** - Full feature roadmap
- **[Core API](core.md)** - Shared core abstractions
