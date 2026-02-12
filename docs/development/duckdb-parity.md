# DuckDB Parity Roadmap

Feature parity tracking for `vw/duckdb/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

DuckDB implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, focusing on DuckDB-specific features and syntax differences.

**Shared with PostgreSQL (via inheritance):**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
- Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- Set operations (UNION, INTERSECT, EXCEPT)
- CTEs (Common Table Expressions)
- Subqueries and conditional expressions
- Parameters and rendering
- Most SQL standard features

**DuckDB-Specific (new implementation required):**
- Star extensions (EXCLUDE, REPLACE, RENAME)
- File I/O (read_csv, read_parquet, COPY statements)
- DuckDB-specific types (LIST, STRUCT, MAP, UNION)
- DuckDB-specific functions (list operations, struct operations)
- Sampling (USING SAMPLE clause)
- Syntax variations and parameter style differences

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in DuckDB:

### ✅ Phase 1: Core Query Building (inherited)
- ✅ SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- ✅ LIMIT, OFFSET, FETCH, DISTINCT
- ✅ Column references and aliasing
- ✅ Immutable dataclass pattern
- ✅ Factory pattern for type safety

### ✅ Phase 2: Operators & Expressions (inherited)
- ✅ Comparison operators (==, !=, <, <=, >, >=)
- ✅ Pattern matching (LIKE, NOT LIKE, IN, NOT IN, BETWEEN)
- ✅ NULL checks (IS NULL, IS NOT NULL)
- ✅ Logical operators (AND, OR, NOT)
- ✅ Mathematical operators (+, -, *, /, %)
- ✅ Generic operator via `.op()`

### ✅ Phase 3: Aggregate & Window Functions (inherited)
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ Window functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- ✅ Window OVER clause with PARTITION BY and ORDER BY
- ✅ Window frame clauses (ROWS BETWEEN, RANGE BETWEEN)
- ✅ FILTER clause for aggregates

### ✅ Phase 4: Joins (inherited)
- ✅ Join types (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- ✅ ON and USING clauses
- ✅ Join chaining

### ✅ Phase 5: Advanced Features (inherited)
- ✅ Subqueries in FROM, WHERE, and SELECT
- ✅ VALUES clause
- ✅ CASE WHEN expressions
- ✅ Set operations (UNION, INTERSECT, EXCEPT)
- ✅ CTEs (Common Table Expressions)
- ✅ Recursive CTEs

### ✅ Phase 6: Parameters & Rendering (inherited)
- ✅ Parameter support with type validation
- ✅ Multiple parameter styles (DOLLAR is default for DuckDB: `$1, $2`)
- ✅ Rendering system with RenderContext
- ⚠️ **DuckDB-specific rendering overrides required**

### ✅ Phase 7: Scalar Functions (inherited)
- ✅ String functions (UPPER, LOWER, TRIM, LENGTH, SUBSTRING, REPLACE, CONCAT)
- ✅ Date/time functions (CURRENT_TIMESTAMP, CURRENT_DATE, EXTRACT)
- ✅ Null handling (COALESCE, NULLIF, GREATEST, LEAST)
- ✅ Type casting

---

## 📋 Phase 1: Core DuckDB Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/duckdb/` directory
- [ ] Create `vw/duckdb/base.py` with DuckDB-specific classes
- [ ] Create `vw/duckdb/public.py` for public API
- [ ] Create `vw/duckdb/render.py` for DuckDB SQL rendering
- [ ] Set up `vw/duckdb/__init__.py` exports
- [ ] Create `tests/duckdb/` directory structure

### DuckDB-Specific Rendering
- [ ] Handle DuckDB identifier quoting (double quotes)
- [ ] DuckDB parameter style (default: `$1`, `$2`, etc.) - same as PostgreSQL
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations (if any)
- [ ] Override PostgreSQL-specific features that don't apply to DuckDB

### Testing Setup
- [ ] Set up test database fixtures (DuckDB in-memory)
- [ ] Port relevant postgres tests to duckdb
- [ ] Create DuckDB-specific test utilities
- [ ] Verify all inherited features work correctly

### Data Structures Needed
- [ ] DuckDB dialect classes inheriting from postgres
- [ ] Override classes for DuckDB-specific behavior

### Examples
```python
from vw.duckdb import source, col, render

# Basic query (inherited from postgres/core)
query = source("users").select(col("name"), col("email"))
sql = render(query)
# Renders: SELECT name, email FROM users
```

---

## 📋 Phase 2: Star Extensions (DuckDB-Specific)

**Status:** ❌ Not Started
**Priority:** HIGH - This is a key DuckDB feature

### Star EXCLUDE
- [ ] `SELECT * EXCLUDE (col1, col2) FROM table`
- [ ] Via `source.star.exclude("col1", "col2")`
- [ ] Rendering with EXCLUDE clause
- [ ] Type checking and validation

### Star REPLACE
- [ ] `SELECT * REPLACE (expr AS col) FROM table`
- [ ] Via `source.star.replace(col=expr)`
- [ ] Multiple replacements
- [ ] Works with qualified stars: `source.star.replace(...)`

### Star RENAME
- [ ] `SELECT * RENAME (old AS new) FROM table` (if DuckDB supports)
- [ ] Via `source.star.rename(old="new")`
- [ ] Research: verify DuckDB supports RENAME or use REPLACE workaround

### Data Structures Needed
- [ ] StarExclude dataclass (columns to exclude)
- [ ] StarReplace dataclass (column replacements as dict)
- [ ] StarRename dataclass (column renames as dict)
- [ ] Enhanced Star dataclass with modifiers
- [ ] StarAccessor class for fluent API on source.star

### Examples
```python
# EXCLUDE
source("users").select(
    source.star.exclude("password", "ssn")
)
# Renders: SELECT * EXCLUDE (password, ssn) FROM users

# REPLACE
source("users").select(
    source.star.replace(
        name=F.upper(col("name")),
        age=col("age") + 1
    )
)
# Renders: SELECT * REPLACE (UPPER(name) AS name, age + 1 AS age) FROM users

# RENAME
source("users").select(
    source.star.rename(old_name="new_name")
)
# Renders: SELECT * RENAME (old_name AS new_name) FROM users
```

### Testing
- [ ] Unit tests for star modifiers
- [ ] Integration tests with real DuckDB
- [ ] Test combinations (EXCLUDE + REPLACE)
- [ ] Test with joins and qualified columns

---

## 📋 Phase 3: File I/O (DuckDB-Specific)

**Status:** ❌ Not Started
**Priority:** HIGH - Core DuckDB value proposition

### READ Functions
- [ ] `read_csv(path, **options)` - read CSV as row source
- [ ] `read_parquet(path, **options)` - read Parquet as row source
- [ ] `read_json(path, **options)` - read JSON as row source
- [ ] Support for S3/HTTP URLs (if DuckDB supports)
- [ ] Options: header, delimiter, null_string, compression, etc.
- [ ] Use as row source: `read_csv(...).select(...)`

### COPY FROM
- [ ] `COPY table FROM 'file.csv'` via `source("table").copy_from(path)`
- [ ] Format options (CSV, Parquet, JSON)
- [ ] Column list specification
- [ ] Options (header, delimiter, null_string, etc.)

### COPY TO
- [ ] `COPY table TO 'file.csv'` via `source("table").copy_to(path)`
- [ ] `COPY query TO 'file.csv'` via `query.copy_to(path)`
- [ ] Format options (CSV, Parquet, JSON)
- [ ] Compression options (gzip, snappy, zstd)

### Data Structures Needed
- [ ] ReadFunction dataclass (extends RowSet)
- [ ] CopyFrom dataclass (statement type)
- [ ] CopyTo dataclass (statement type)
- [ ] FormatOptions dataclass (or dict)

### Examples
```python
# Read CSV as source
read_csv("users.csv").select(col("name"), col("email"))
# Renders: SELECT name, email FROM read_csv('users.csv')

# With options
read_csv("users.csv", header=True, delimiter=",").where(col("age") > 18)

# Copy from file
source("users").copy_from("users.csv", format="csv", header=True)
# Renders: COPY users FROM 'users.csv' (FORMAT CSV, HEADER)

# Copy query results to file
source("users").select(col("name")).copy_to("names.csv")
# Renders: COPY (SELECT name FROM users) TO 'names.csv'
```

### Testing
- [ ] Unit tests for data structures
- [ ] Integration tests with actual file I/O
- [ ] Test various formats (CSV, Parquet, JSON)
- [ ] Test options and error handling

---

## 📋 Phase 4: DuckDB-Specific Types

**Status:** ❌ Not Started
**Priority:** MEDIUM

### DuckDB Native Types
- [ ] LIST type (arrays)
- [ ] STRUCT type (named fields)
- [ ] MAP type (key-value pairs)
- [ ] UNION type (tagged union)
- [ ] ENUM type
- [ ] BIT type
- [ ] BLOB type
- [ ] INTERVAL type

### Type System Integration
- [ ] Type constructors in `vw.duckdb.types`
- [ ] Type casting to DuckDB types
- [ ] Type inference for literals
- [ ] Type validation

### Examples
```python
from vw.duckdb import types as T

# LIST type
col("tags").cast(T.list(T.varchar()))

# STRUCT type
col("address").cast(T.struct(street=T.varchar(), city=T.varchar()))

# MAP type
col("metadata").cast(T.map(T.varchar(), T.varchar()))
```

### Testing
- [ ] Unit tests for type constructors
- [ ] Integration tests with type casting
- [ ] Test type inference

---

## 📋 Phase 5: List/Array Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM-HIGH

### List Construction
- [ ] `list_value([1, 2, 3])` - create list literal
- [ ] List literals via Python lists
- [ ] `array_agg()` - aggregate to array (inherited from postgres)

### List Operations
- [ ] `list_extract(list, index)` - access list element
- [ ] `list_slice(list, start, end)` - slice list
- [ ] `list_contains(list, element)` - check membership
- [ ] `list_concat(list1, list2)` - concatenate lists
- [ ] `list_append(list, element)` - append element
- [ ] `list_prepend(element, list)` - prepend element
- [ ] `unnest(list)` - expand list to rows (inherited from postgres)

### List Aggregates
- [ ] `list_agg(expr)` - DuckDB-specific list aggregation
- [ ] Support for ORDER BY within list_agg

### Data Structures Needed
- [ ] ListValue dataclass for list literals
- [ ] List function wrappers in F module

### Examples
```python
# List operations
col("tags").list.extract(1)  # Access first element
col("tags").list.slice(1, 3)  # Slice
col("tags").list.contains("python")  # Check membership

# List aggregation
F.list_agg(col("name"), order_by=[col("id").asc()])
```

### Testing
- [ ] Unit tests for list operations
- [ ] Integration tests with actual lists
- [ ] Test list aggregation with ORDER BY

---

## 📋 Phase 6: Struct Operations

**Status:** ❌ Not Started
**Priority:** MEDIUM

### Struct Construction
- [ ] `struct_pack(field1=val1, field2=val2)` - create struct
- [ ] Struct literals via dict notation
- [ ] ROW constructor (SQL standard)

### Struct Access
- [ ] `struct_extract(struct, field)` - access struct field
- [ ] Dot notation for field access (if possible)
- [ ] `struct.field` accessor pattern

### Struct Functions
- [ ] `struct_insert(struct, field=value)` - add/update field
- [ ] `struct_keys(struct)` - get field names
- [ ] `struct_values(struct)` - get field values

### Data Structures Needed
- [ ] StructValue dataclass for struct literals
- [ ] StructAccessor for field access

### Examples
```python
# Struct construction
F.struct_pack(name="Alice", age=30)

# Struct access
col("address").struct.extract("city")
col("address").struct.city  # If possible
```

### Testing
- [ ] Unit tests for struct operations
- [ ] Integration tests with nested structs

---

## 📋 Phase 7: Sampling

**Status:** ❌ Not Started
**Priority:** MEDIUM

### Sampling Methods
- [ ] `USING SAMPLE` clause
- [ ] Percentage sampling via `.sample(percent=10)`
- [ ] Row count sampling via `.sample(rows=1000)`
- [ ] Reservoir sampling via `.sample(method="reservoir", rows=1000)`
- [ ] Bernoulli sampling via `.sample(method="bernoulli", percent=10)`
- [ ] System sampling via `.sample(method="system", percent=10)`

### Data Structures Needed
- [ ] Sample dataclass (method, percent, rows, seed)
- [ ] Sampling methods on Statement

### Examples
```python
# Percentage sampling
source("big_table").select(col("id")).sample(percent=10)
# Renders: SELECT id FROM big_table USING SAMPLE 10%

# Row count sampling
source("big_table").select(col("id")).sample(rows=1000)
# Renders: SELECT id FROM big_table USING SAMPLE 1000 ROWS

# With seed for reproducibility
source("big_table").sample(percent=10, seed=42)
# Renders: SELECT * FROM big_table USING SAMPLE 10% (SEED 42)
```

### Testing
- [ ] Unit tests for sample clauses
- [ ] Integration tests with different sampling methods
- [ ] Test seed reproducibility

---

## 📋 Phase 8: DuckDB-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### String Functions (DuckDB-specific)
- [ ] `regexp_matches()` - DuckDB regex
- [ ] `regexp_replace()` - DuckDB regex replace
- [ ] `regexp_extract()` - DuckDB regex extract
- [ ] `string_split()` - split string to list
- [ ] `string_split_regex()` - split by regex

### JSON Functions (DuckDB-specific)
- [ ] DuckDB JSON extraction functions
- [ ] JSON path queries
- [ ] JSON operators (via `.op()`)

### Date/Time Functions (DuckDB-specific)
- [ ] `date_diff(unit, start, end)` - date difference
- [ ] `date_add(date, interval)` - add interval
- [ ] `date_sub(date, interval)` - subtract interval
- [ ] `date_part(unit, date)` - extract date part
- [ ] `make_date(year, month, day)` - construct date
- [ ] `make_time(hour, minute, second)` - construct time
- [ ] `make_timestamp(...)` - construct timestamp

### Statistical Functions
- [ ] `approx_count_distinct()` - approximate distinct count
- [ ] `approx_quantile()` - approximate quantile
- [ ] `mode()` - most frequent value
- [ ] `median()` - median value
- [ ] `quantile()` - quantile value
- [ ] `reservoir_quantile()` - approximate quantile using reservoir sampling

### Data Structures Needed
- [ ] Function wrappers in F module
- [ ] DuckDB-specific accessor methods

### Examples
```python
# String functions
col("text").text.split(",")
col("text").text.regexp_extract(r"(\d+)")

# Statistical functions
F.median(col("age"))
F.approx_quantile(col("value"), 0.95)
```

### Testing
- [ ] Unit tests for each function
- [ ] Integration tests with real data

---

## 📋 Phase 9: DuckDB Advanced Features

**Status:** ❌ Not Started
**Priority:** LOW

### CREATE TABLE AS (CTAS)
- [ ] `CREATE TABLE ... AS SELECT`
- [ ] `CREATE OR REPLACE TABLE`
- [ ] Temporary tables
- [ ] Via `.create_table(name, or_replace=False, temporary=False)`

### Sequences
- [ ] CREATE SEQUENCE
- [ ] `nextval()` function
- [ ] `currval()` function
- [ ] Sequence usage in INSERT

### PIVOT/UNPIVOT
- [ ] PIVOT support (if DuckDB has it)
- [ ] UNPIVOT support
- [ ] Via `.pivot()` and `.unpivot()` methods

### Extensions System
- [ ] INSTALL extension via `install_extension(name)`
- [ ] LOAD extension via `load_extension(name)`
- [ ] Common extensions (httpfs, parquet, json, excel)

### Configuration
- [ ] SET statements
- [ ] PRAGMA statements
- [ ] Configuration via Python API

### Information Commands
- [ ] DESCRIBE table
- [ ] SHOW tables
- [ ] SHOW functions
- [ ] Via utility functions

### Data Structures Needed
- [ ] CreateTable dataclass
- [ ] Pivot/Unpivot dataclasses
- [ ] Extension management utilities

### Examples
```python
# Create table from query
source("users").select(col("name"), col("email")).create_table("user_emails")
# Renders: CREATE TABLE user_emails AS SELECT name, email FROM users

# Load extension
load_extension("httpfs")
```

### Testing
- [ ] Unit tests for DDL statements
- [ ] Integration tests for table creation
- [ ] Test extension loading

---

## 📋 Phase 10: DuckDB Operators

**Status:** ❌ Not Started
**Priority:** LOW

### List Operators
- [ ] List construction `[1, 2, 3]` syntax
- [ ] List concatenation `||`
- [ ] List indexing `list[index]`
- [ ] All via `.op()` or dedicated methods

### Struct Operators
- [ ] Struct field access `.field`
- [ ] Struct construction
- [ ] Via `.struct` accessor

### Pattern Matching
- [ ] SIMILAR TO operator (if different from postgres)
- [ ] DuckDB regex operators
- [ ] Via `.op()` method

### JSON/Array Operators
- [ ] `->` operator for JSON/struct access (via `.op()`)
- [ ] `->>` operator for JSON/struct text access (via `.op()`)
- [ ] `@>` operator for containment (via `.op()`)
- [ ] All available via existing `.op()` method

---

## Key Differences from PostgreSQL

### Syntax Differences
- ✅ Parameter style: Same as PostgreSQL (`$1, $2`) for prepared statements
- ⚠️ Type names: DuckDB has TINYINT, UTINYINT, HUGEINT, etc.
- ⚠️ Star extensions: EXCLUDE, REPLACE are DuckDB-specific
- ⚠️ Identifier quoting: Double quotes (same as PostgreSQL)
- ⚠️ Some function names differ

### Feature Differences
- ➕ Star extensions (EXCLUDE, REPLACE, RENAME)
- ➕ Built-in file reading (read_csv, read_parquet, read_json)
- ➕ Native LIST, STRUCT, MAP types
- ➕ USING SAMPLE clause for sampling
- ➕ Different performance tuning options
- ➖ No ILIKE operator (use regexp_matches or LOWER + LIKE)
- ➖ Different full-text search (no tsvector/tsquery)
- ➖ No PostGIS/geometric types (different extension model)

### Rendering Differences
- Some functions have different names
- Type casting syntax may differ slightly
- DuckDB is columnar (affects optimization strategies)

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure from `vw/core/` and `vw/postgres/`
- Focus on DuckDB-specific features only
- Override PostgreSQL-specific features that don't apply

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering (override postgres where needed)
- **Phase 2:** Star extensions (highest value DuckDB feature) 🌟
- **Phase 3:** File I/O (second highest value) 🌟
- **Phase 4-10:** Remaining features as needed

### 3. Code Reuse Strategy
```python
# vw/duckdb/base.py inherits from postgres
from vw.postgres.base import (
    source as pg_source,
    col as pg_col,
    # ... other imports
)

# Override only what's different
def source(name: str, **kwargs):
    """DuckDB source with star extension support."""
    return pg_source(name, **kwargs)  # Add DuckDB-specific behavior
```

### 4. Testing Strategy
- Port all postgres tests to verify inheritance works
- Add DuckDB-specific tests for new features
- Use same test patterns (sql() utility, fixtures)
- Test with in-memory DuckDB database

### 5. Raw SQL API Inheritance
- ✅ `raw.expr()` and `raw.rowset()` inherited from postgres
- ✅ `raw.func()` inherited from postgres
- Use for DuckDB-specific features not yet wrapped

---

## Current Status Summary

**Completed:**
- None yet ❌ (waiting on PostgreSQL completion)

**Inherited from PostgreSQL (when available):**
- ✅ Phase 1: Core Query Building
- ✅ Phase 2: Operators & Expressions
- ✅ Phase 3: Aggregate & Window Functions
- ✅ Phase 4: Joins
- ✅ Phase 5: Advanced Features (Subqueries, VALUES, CASE, Set Operations, CTEs)
- ✅ Phase 6: Parameters & Rendering
- ✅ Phase 7: Scalar Functions

**DuckDB-Specific (to be implemented):**
- ❌ Phase 1: Core DuckDB Implementation (infrastructure)
- ❌ Phase 2: Star Extensions (EXCLUDE, REPLACE, RENAME) 🌟 HIGH PRIORITY
- ❌ Phase 3: File I/O (read_csv, read_parquet, COPY) 🌟 HIGH PRIORITY
- ❌ Phase 4: DuckDB-Specific Types (LIST, STRUCT, MAP, UNION)
- ❌ Phase 5: List/Array Functions
- ❌ Phase 6: Struct Operations
- ❌ Phase 7: Sampling (USING SAMPLE)
- ❌ Phase 8: DuckDB-Specific Functions
- ❌ Phase 9: DuckDB Advanced Features (CTAS, Extensions, etc.)
- ❌ Phase 10: DuckDB Operators

**Estimated Start:**
- After PostgreSQL Phase 7 complete (~80% of postgres features)
- DuckDB can share most infrastructure via inheritance

**Total Progress:** 0% complete (waiting on postgres infrastructure)

---

## Priority Matrix

### HIGH Priority (Core DuckDB Value)
1. ⭐ Star extensions (EXCLUDE, REPLACE) - unique to DuckDB
2. ⭐ File I/O (read_csv, read_parquet, COPY) - core use case

### MEDIUM Priority (Common Use Cases)
3. List/array operations - used frequently
4. DuckDB-specific types - needed for proper type handling
5. Sampling - useful for large datasets
6. Struct operations - needed for nested data

### LOW Priority (Nice to Have)
7. DuckDB-specific functions - use raw.func() until needed
8. Advanced features (CTAS, extensions, PIVOT)
9. Specialized operators - use .op() until needed

**Recommendation:** Focus on HIGH and MEDIUM priorities first. LOW priority items can be handled via raw SQL API (`raw.expr()`, `raw.func()`) until demand justifies dedicated wrappers.