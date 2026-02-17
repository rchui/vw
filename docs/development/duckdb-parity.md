# DuckDB Parity Roadmap

Feature parity tracking for `vw/duckdb/` implementation.

**Status:** ✅ Phase 1, 2, 3a, & 4 Complete - Core infrastructure, Star Extensions, File Reading, and Type System implemented
**Current Phase:** Phase 5 - List/Array Functions (next priority)
**Prerequisites:** Most PostgreSQL phases must be completed first (✅ Complete)

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

## ✅ Phase 1: Core DuckDB Implementation

**Status:** ✅ Complete

### Infrastructure Setup
- [x] Create `vw/duckdb/` directory
- [x] Create `vw/duckdb/base.py` with DuckDB-specific classes
- [x] Create `vw/duckdb/public.py` for public API
- [x] Create `vw/duckdb/render.py` for DuckDB SQL rendering
- [x] Set up `vw/duckdb/__init__.py` exports
- [x] Create `tests/duckdb/` directory structure

### DuckDB-Specific Rendering
- [x] Handle DuckDB identifier quoting (double quotes)
- [x] DuckDB parameter style (default: `$1`, `$2`, etc.) - same as PostgreSQL
- [x] Type name differences from PostgreSQL
- [x] Function name variations (if any)
- [x] Override PostgreSQL-specific features that don't apply to DuckDB

### Testing Setup
- [x] Set up test database fixtures (DuckDB in-memory)
- [x] Port relevant postgres tests to duckdb
- [x] Create DuckDB-specific test utilities
- [x] Verify all inherited features work correctly

### Data Structures Needed
- [x] DuckDB dialect classes inheriting from postgres
- [x] Override classes for DuckDB-specific behavior

### Implementation Notes
- Successfully inherited core query building from `vw/core` and `vw/postgres`
- DuckDB-specific rendering implemented with proper identifier quoting
- All ANSI SQL features working: SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- Joins (INNER, LEFT, RIGHT, FULL, CROSS), subqueries, CTEs, and set operations all functional
- Aggregate and window functions working correctly
- Parameter support using dollar-style (`$1`, `$2`) matching DuckDB conventions

### Examples
```python
from vw.duckdb import ref, col, render

# Basic query (inherited from postgres/core)
query = ref("users").select(col("name"), col("email"))
sql = render(query)
# Renders: SELECT name, email FROM users

# Complex query with joins and aggregates
from vw.duckdb import F

query = (
    ref("users").alias("u")
    .select(col("u.name"), F.count(col("o.id")).alias("order_count"))
    .join.inner(ref("orders").alias("o"), on=[(col("u.id") == col("o.user_id"))])
    .group_by(col("u.name"))
    .having(F.count(col("o.id")) > 5)
)
sql = render(query)
# Full SQL with proper DuckDB syntax
```

---

## ✅ Phase 2: Star Extensions (DuckDB-Specific)

**Status:** ✅ Complete
**Priority:** HIGH - This is a key DuckDB feature

### Star EXCLUDE
- [x] `SELECT * EXCLUDE (col1, col2) FROM table`
- [x] Via `rowset.star.exclude(col("col1"), col("col2"))`
- [x] Rendering with EXCLUDE clause
- [x] Type checking and validation

### Star REPLACE
- [x] `SELECT * REPLACE (expr AS col) FROM table`
- [x] Via `rowset.star.replace(col=expr)`
- [x] Multiple replacements
- [x] Works with qualified stars: `rowset.star.replace(...)`

### Star RENAME
- ❌ Not implemented - DuckDB doesn't support RENAME in star expressions (can use REPLACE as workaround)

### Data Structures Needed
- [x] StarExclude dataclass (columns to exclude)
- [x] StarReplace dataclass (column replacements as dict)
- [x] Enhanced Star dataclass with modifiers
- [x] StarAccessor class for fluent API on rowset.star

### Implementation Notes
- Implemented in `vw/duckdb/states.py` (Star, StarExclude, StarReplace)
- StarAccessor in `vw/duckdb/star.py` provides fluent API
- Rendering in `vw/duckdb/render.py` handles all modifier combinations
- Supports both qualified (`table.*`) and aliased (`alias.*`) stars
- Modifiers can be combined: `EXCLUDE (...) REPLACE (...)`
- Modifier order is preserved (useful for semantic control)
- Works seamlessly in SELECT, with JOINs, and alongside other columns

### Examples
```python
from vw.duckdb import ref, col, F, render

# EXCLUDE
users = ref("users")
query = users.select(users.star(users.star.exclude(col("password"), col("ssn"))))
result = render(query)
# Renders: SELECT users.* EXCLUDE (password, ssn) FROM users

# REPLACE
query = users.select(
    users.star(
        users.star.replace(
            name=F.upper(col("name")),
            age=col("age") + 1
        )
    )
)
result = render(query)
# Renders: SELECT users.* REPLACE (UPPER(name) AS name, age + 1 AS age) FROM users

# Combined EXCLUDE and REPLACE
u = ref("users").alias("u")
query = u.select(
    u.star(
        u.star.exclude(col("password"), col("secret")),
        u.star.replace(name=col("full_name"))
    )
)
result = render(query)
# Renders: SELECT u.* EXCLUDE (password, secret) REPLACE (full_name AS name) FROM users AS u

# With JOINs
users = ref("users").alias("u")
orders = ref("orders").alias("o")
query = (
    users
    .select(
        users.star(users.star.exclude(col("password"))),
        col("o.total")
    )
    .join.inner(orders, on=[(col("u.id") == col("o.user_id"))])
)
result = render(query)
# Renders: SELECT u.* EXCLUDE (password), o.total FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
```

### Testing
- [x] Unit tests for star modifiers in `tests/duckdb/test_star.py`
- [x] Integration tests with DuckDB rendering
- [x] Test combinations (EXCLUDE + REPLACE in both orders)
- [x] Test with joins and qualified columns
- [x] Test with aliases and CTEs
- [x] Error handling for unsupported source/modifier types

---

## ✅ Phase 3a: File Reading (DuckDB-Specific)

**Status:** ✅ Complete
**Priority:** HIGH - Core DuckDB value proposition

### READ Functions
- [x] `read_csv(path, **options)` - read CSV as row source via `file(path, format=CSV(...))`
- [x] `read_parquet(path, **options)` - read Parquet as row source via `file(path, format=Parquet(...))`
- [x] `read_json(path, **options)` - read JSON as row source via `file(path, format=JSON(...))`
- [x] `read_json(path, format='newline_delimited', **options)` - read JSONL via `file(path, format=JSONL(...))`
- [x] Multiple file paths and wildcards supported
- [x] Comprehensive options for each format (22 CSV options, 6 Parquet, 7 JSON/JSONL)
- [x] Use as row source: `file(...).select(...)`, joins, CTEs, etc.
- [ ] Support for S3/HTTP URLs (deferred - DuckDB supports, but not yet wrapped)

### Data Structures Implemented
- [x] `File` state in `vw/core/states.py` (generic file reading concept)
- [x] `CSV` format modifier in `vw/duckdb/files.py` with 22 options
- [x] `Parquet` format modifier with 6 options
- [x] `JSON` format modifier with 7 options
- [x] `JSONL` format modifier (renders with `format='newline_delimited'`)
- [x] `file(*paths, format)` factory function
- [x] Rendering in `vw/duckdb/render.py` with helper functions
- [x] Error handling with `RenderError` for unsupported sources

### Implementation Notes
- File state moved to core for reusability, format modifiers kept dialect-specific
- Pattern matching on format type to determine DuckDB function (read_csv, read_parquet, read_json)
- Helper functions for rendering different option types (boolean, string, integer, dict, list)
- Works seamlessly with SELECT, WHERE, JOIN, CTEs, and all query operations
- Postgres render explicitly rejects File (raises RenderError) to enforce dialect separation
- File is automatically transformed to Statement when .select() or other query methods are called

### Examples
```python
from vw.duckdb import file, CSV, Parquet, JSON, JSONL, col, F, render

# Read CSV as source
query = file("users.csv", format=CSV(header=True)).select(col("name"), col("email"))
result = render(query)
# Renders: SELECT name, email FROM read_csv('users.csv', header = TRUE)

# With multiple options
query = file("data.csv", format=CSV(header=True, delim="|", skip=1, all_varchar=True))
# Renders: FROM read_csv('data.csv', header = TRUE, delim = '|', skip = 1, all_varchar = TRUE)

# Read Parquet
query = file("data.parquet", format=Parquet(filename=True))
# Renders: FROM read_parquet('data.parquet', filename = TRUE)

# Read JSON
query = file("data.json", format=JSON(ignore_errors=True, compression="gzip"))
# Renders: FROM read_json('data.json', ignore_errors = TRUE, compression = 'gzip')

# Read JSONL (newline-delimited JSON)
query = file("events.jsonl", format=JSONL()).select(col("event_type"))
# Renders: SELECT event_type FROM read_json('events.jsonl', format = 'newline_delimited')

# Multiple files
query = file("f1.csv", "f2.csv", format=CSV(header=True))
# Renders: FROM read_csv(['f1.csv', 'f2.csv'], header = TRUE)

# Wildcards
query = file("data/*.csv", format=CSV(header=True, union_by_name=True))
# Renders: FROM read_csv('data/*.csv', header = TRUE, union_by_name = TRUE)

# With joins
from vw.duckdb import ref
users = ref("users").alias("u")
scores = file("scores.csv", format=CSV(header=True)).alias("f")
query = users.select(users.col("name"), scores.col("score")).join.inner(
    scores, on=[users.col("id") == scores.col("user_id")]
)
# Renders: SELECT u.name, f.score FROM users AS u INNER JOIN read_csv('scores.csv', header = TRUE) AS f ON (u.id = f.user_id)

# With CTEs
from vw.duckdb import cte, lit
high_scores = cte(
    "user_scores",
    file("scores.csv", format=CSV(header=True))
    .select(col("name"), col("score"))
    .where(col("score") > lit(90))
)
query = high_scores.select(col("name"), col("score")).order_by(col("score").desc())
# Renders: WITH user_scores AS (SELECT name, score FROM read_csv('scores.csv', header = TRUE) WHERE score > 90) SELECT name, score FROM user_scores ORDER BY score DESC
```

### Testing
- [x] 54 unit tests in `tests/duckdb/test_files.py` (all passing)
  - File factory tests
  - CSV rendering (basic, string, boolean, integer, complex options)
  - Parquet, JSON, JSONL format tests
  - Multiple files tests
  - Integration with SELECT, WHERE, aggregation, ORDER BY, LIMIT, alias
- [x] 17 integration tests in `tests/duckdb/integration/test_file_reading.py` (all passing)
  - CSV reading with various options
  - Parquet reading
  - JSON and JSONL reading
  - Multiple file reading with wildcards
  - File reading with joins and CTEs
- [x] All quality checks passing (ruff, type checking)
- [x] Full test suite passing (1312 tests)

---

## 📋 Phase 3b: COPY Statements (DuckDB-Specific)

**Status:** ❌ Not Started (Deferred)
**Priority:** MEDIUM - Useful but not core workflow

### COPY FROM
- [ ] `COPY table FROM 'file.csv'` via `ref("table").copy_from(path)`
- [ ] Format options (CSV, Parquet, JSON)
- [ ] Column list specification
- [ ] Options (header, delimiter, null_string, etc.)

### COPY TO
- [ ] `COPY table TO 'file.csv'` via `ref("table").copy_to(path)`
- [ ] `COPY query TO 'file.csv'` via `query.copy_to(path)`
- [ ] Format options (CSV, Parquet, JSON)
- [ ] Compression options (gzip, snappy, zstd)

### Data Structures Needed
- [ ] CopyFrom dataclass (statement type)
- [ ] CopyTo dataclass (statement type)

### Examples
```python
# Copy from file (future API)
ref("users").copy_from("users.csv", format="csv", header=True)
# Renders: COPY users FROM 'users.csv' (FORMAT CSV, HEADER)

# Copy query results to file (future API)
ref("users").select(col("name")).copy_to("names.csv")
# Renders: COPY (SELECT name FROM users) TO 'names.csv'
```

### Testing
- [ ] Unit tests for COPY statements
- [ ] Integration tests with file I/O
- [ ] Test various formats and options

---

## ✅ Phase 4: DuckDB-Specific Types

**Status:** ✅ Complete
**Priority:** MEDIUM

### DuckDB Native Types
- [x] LIST type (variable-length arrays)
- [x] STRUCT type (named fields)
- [x] MAP type (key-value pairs)
- [x] ARRAY type (fixed-length arrays)
- [x] Integer variants (TINYINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT)
- [x] BLOB, BIT, INTERVAL types
- [x] FLOAT, DOUBLE aliases
- [ ] UNION type (deferred - rarely used)
- [ ] ENUM type (deferred - requires CREATE TYPE)

### Type System Integration
- [x] Type constructors in `vw.duckdb.types`
- [x] Type casting to DuckDB types via `.cast()`
- [x] T shorthand for ergonomic usage (`from vw.duckdb import T`)
- [x] Nested type composition (LIST of STRUCT, etc.)
- [x] Re-export all core ANSI SQL types

### Implementation Notes
- Type functions return simple SQL type strings
- DuckDB uses angle bracket syntax for parameterized types: `LIST<VARCHAR>`, `STRUCT<name: VARCHAR>`
- Cast rendering works via existing `::` syntax
- T shorthand provides ergonomic access: `T.LIST(T.VARCHAR())`
- No type validation - DuckDB handles validation at runtime
- Value construction deferred to Phase 5 (List Functions) and Phase 6 (Struct Operations)

### Examples
```python
from vw.duckdb import T, col, ref, render

# Simple DuckDB types
col("age").cast(T.TINYINT())      # 8-bit integer
col("count").cast(T.HUGEINT())    # 128-bit integer

# LIST type (variable-length arrays)
col("tags").cast(T.LIST(T.VARCHAR()))
# Renders: tags::LIST<VARCHAR>

# STRUCT type (named fields)
col("address").cast(T.STRUCT({"street": T.VARCHAR(), "city": T.VARCHAR(), "zip": T.INTEGER()}))
# Renders: address::STRUCT<street: VARCHAR, city: VARCHAR, zip: INTEGER>

# MAP type (key-value pairs)
col("metadata").cast(T.MAP(T.VARCHAR(), T.VARCHAR()))
# Renders: metadata::MAP<VARCHAR, VARCHAR>

# ARRAY type (fixed-length)
col("coords").cast(T.ARRAY(T.DOUBLE(), 3))
# Renders: coords::DOUBLE[3]

# Nested types
col("matrix").cast(T.LIST(T.LIST(T.INTEGER())))
# Renders: matrix::LIST<LIST<INTEGER>>

# Complex composition
col("data").cast(T.STRUCT({
    "id": T.INTEGER(),
    "tags": T.LIST(T.VARCHAR()),
    "metadata": T.MAP(T.VARCHAR(), T.INTEGER())
}))
# Renders: data::STRUCT<id: INTEGER, tags: LIST<VARCHAR>, metadata: MAP<VARCHAR, INTEGER>>
```

### Testing
- [x] 65 unit tests in `tests/duckdb/test_types.py` (all passing)
  - Core type re-exports (23 tests)
  - DuckDB integer variants (7 tests)
  - DuckDB simple types (5 tests)
  - LIST type (6 tests)
  - STRUCT type (8 tests)
  - MAP type (6 tests)
  - ARRAY type (5 tests)
  - Type composition (5 tests)
- [x] 21 integration tests in `tests/duckdb/integration/test_type_casting.py` (all passing)
  - Cast rendering (9 tests)
  - Cast in queries (5 tests)
  - Complex nested types (4 tests)
  - T shorthand usage (3 tests)
- [x] All quality checks passing (ruff, type checking)

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
- ✅ Phase 1: Core DuckDB Implementation (infrastructure complete)
- ✅ Phase 2: Star Extensions (EXCLUDE, REPLACE) 🌟
- ✅ Phase 3a: File Reading (read_csv, read_parquet, read_json, read_jsonl) 🌟

**Inherited from PostgreSQL:**
- ✅ Phase 1: Core Query Building
- ✅ Phase 2: Operators & Expressions
- ✅ Phase 3: Aggregate & Window Functions
- ✅ Phase 4: Joins
- ✅ Phase 5: Advanced Features (Subqueries, VALUES, CASE, Set Operations, CTEs)
- ✅ Phase 6: Parameters & Rendering
- ✅ Phase 7: Scalar Functions

**DuckDB-Specific (remaining work):**
- ⏸️ Phase 3b: COPY Statements (deferred) - can use raw SQL until needed
- ❌ Phase 4: DuckDB-Specific Types (LIST, STRUCT, MAP, UNION)
- ❌ Phase 5: List/Array Functions 🌟 MEDIUM-HIGH PRIORITY - NEXT
- ❌ Phase 6: Struct Operations
- ❌ Phase 7: Sampling (USING SAMPLE)
- ❌ Phase 8: DuckDB-Specific Functions
- ❌ Phase 9: DuckDB Advanced Features (CTAS, Extensions, etc.)
- ❌ Phase 10: DuckDB Operators

**Total Progress:** 30% complete (3 of 10 DuckDB-specific phases complete, 1 deferred)

**Next Steps:**
- Phase 5: List/Array Functions (list operations, list_agg) - high value for DuckDB users
- Phase 4: DuckDB-Specific Types (LIST, STRUCT, MAP) - needed for proper type handling
- Focus on HIGH and MEDIUM priority features that provide unique DuckDB value

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