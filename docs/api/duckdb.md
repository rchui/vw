# DuckDB API Reference

The `vw.duckdb` module provides DuckDB-specific implementations and rendering.

**Status:** ⚠️ **Incomplete** - Basic structure only, full implementation pending.

## Module Import

```python
from vw.duckdb import source
```

## Factory Functions

### `source(name)`

Create a DuckDB table or view source.

```python
users = source("users")
# Represents: users table
```

**Parameters:**
- `name` (str) - Table or view name

**Returns:** RowSet wrapping Source state

**Status:** ✅ Available

## Missing Features

The following features are not yet implemented for DuckDB:

### Factory Functions
- ❌ `col(name)` - Column reference factory
- ❌ `param(name, value)` - Parameter factory
- ❌ `render(rowset)` - SQL rendering function
- ❌ `F` - Functions instance

### Rendering
- ❌ `vw/duckdb/render.py` - DuckDB SQL rendering logic
- ❌ Parameter style configuration
- ❌ DuckDB-specific syntax handling

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
