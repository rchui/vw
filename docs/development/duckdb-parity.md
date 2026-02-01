# DuckDB Parity Roadmap

Feature parity tracking for `vw/duckdb/` implementation vs `vw/reference/`.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for postgres core to stabilize

---

## Strategy

DuckDB implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, focusing on DuckDB-specific features and syntax differences.

**Shared with Postgres:**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions
- Most SQL standard features

**DuckDB-Specific:**
- Star extensions (EXCLUDE, REPLACE, RENAME)
- COPY statements
- Parquet/CSV file operations
- DuckDB-specific functions
- Syntax variations

---

## Prerequisites (Must Complete First)

These phases from postgres roadmap must be completed before starting DuckDB:

- ✅ Phase 1: Core Query Building (postgres)
- [ ] Phase 2: Operators & Expressions (postgres)
- [ ] Phase 3: Aggregate & Window Functions (postgres)
- [ ] Phase 4: Joins (postgres)
- [ ] Phase 6: Parameters & Rendering (postgres)

---

## 📋 Phase 1: Core DuckDB Implementation

### Infrastructure Setup
- [ ] Create `vw/duckdb/` directory
- [ ] Create `vw/duckdb/base.py` with DuckDB-specific classes
- [ ] Create `vw/duckdb/public.py` for public API
- [ ] Create `vw/duckdb/render.py` for DuckDB SQL rendering
- [ ] Set up `vw/duckdb/__init__.py` exports
- [ ] Create `tests/duckdb/` directory structure

### Basic Rendering Differences
- [ ] Handle DuckDB syntax variations
- [ ] DuckDB parameter style (default: `$1`, `$2`, etc.)
- [ ] Identifier quoting (double quotes by default)
- [ ] Type name differences from PostgreSQL

### Testing Setup
- [ ] Port postgres tests to duckdb
- [ ] Add DuckDB-specific test utilities
- [ ] Set up test database fixtures

---

## 📋 Phase 2: Star Extensions (DuckDB-Specific)

### Star EXCLUDE
- [ ] `SELECT * EXCLUDE (col1, col2) FROM table`
- [ ] Via `source.star.exclude("col1", "col2")`
- [ ] Rendering with EXCLUDE clause

### Star REPLACE
- [ ] `SELECT * REPLACE (expr AS col) FROM table`
- [ ] Via `source.star.replace(col=expr)`
- [ ] Multiple replacements

### Star RENAME
- [ ] DuckDB extension (if available)
- [ ] Via `source.star.rename(old="new")`

### Data Structures Needed
- [ ] StarExclude dataclass
- [ ] StarReplace dataclass
- [ ] Enhanced Star dataclass with modifiers
- [ ] StarAccessor class for fluent API

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
```

---

## 📋 Phase 3: DuckDB Data Import/Export

### COPY FROM
- [ ] COPY table FROM 'file.csv' via `source("table").copy_from(path)`
- [ ] Format options (CSV, Parquet, JSON)
- [ ] Column list
- [ ] Options (header, delimiter, null string, etc.)

### COPY TO
- [ ] COPY table TO 'file.csv' via `source("table").copy_to(path)`
- [ ] COPY query TO 'file.csv' via `query.copy_to(path)`
- [ ] Format options
- [ ] Compression options

### READ Functions
- [ ] read_csv() via `read_csv(path, **options)`
- [ ] read_parquet() via `read_parquet(path)`
- [ ] read_json() via `read_json(path)`
- [ ] Use as row source: `read_csv(...).select(...)`

### WRITE Functions
- [ ] write_csv() for queries
- [ ] write_parquet() for queries

### Data Structures Needed
- [ ] CopyFrom dataclass
- [ ] CopyTo dataclass
- [ ] ReadFunction dataclass (extends RowSet)
- [ ] FormatOptions dataclass

### Examples
```python
# Read CSV as source
read_csv("users.csv").select(col("name"), col("email"))

# Copy from file
source("users").copy_from("users.csv", format="csv", header=True)

# Copy query results to file
source("users").select(col("name")).copy_to("names.csv")
```

---

## 📋 Phase 4: DuckDB-Specific Functions

### String Functions
- [ ] `regexp_matches()` - DuckDB regex
- [ ] `regexp_replace()` - DuckDB regex replace
- [ ] `regexp_extract()` - DuckDB regex extract
- [ ] `list_aggregate()` - DuckDB list functions
- [ ] `string_split()` - Split string to array

### Array/List Functions
- [ ] `list_value()` - Create list literal
- [ ] `list_extract()` - Access list element
- [ ] `list_slice()` - Slice list
- [ ] `unnest()` - Expand list to rows
- [ ] `list_contains()` - Check membership
- [ ] `array_agg()` - Aggregate to array (also in postgres)

### Struct Functions
- [ ] `struct_pack()` - Create struct
- [ ] `struct_extract()` - Access struct field
- [ ] Struct literal syntax

### JSON Functions
- [ ] DuckDB JSON functions
- [ ] JSON extraction operators

### Date/Time Functions
- [ ] DuckDB-specific date functions
- [ ] Time zone handling
- [ ] Date arithmetic variations

---

## 📋 Phase 5: DuckDB Advanced Features

### Sampling
- [ ] `USING SAMPLE` clause
- [ ] Percentage sampling via `.sample(percent=10)`
- [ ] Row count sampling via `.sample(rows=1000)`
- [ ] Reservoir sampling
- [ ] Bernoulli sampling

### Materialization
- [ ] CREATE OR REPLACE TABLE via `.or_replace()`
- [ ] CREATE TABLE ... AS SELECT
- [ ] Temporary tables

### Sequences
- [ ] CREATE SEQUENCE
- [ ] nextval(), currval()
- [ ] Sequence usage in INSERT

### Pivoting
- [ ] PIVOT support (if DuckDB has it)
- [ ] UNPIVOT support

### Data Structures Needed
- [ ] Sample dataclass
- [ ] Sampling methods on Statement

### Examples
```python
# Sampling
source("big_table").select(col("id")).sample(percent=10)
# Renders: SELECT id FROM big_table USING SAMPLE 10%
```

---

## 📋 Phase 6: DuckDB-Specific Operators

### List Operators
- [ ] List construction `[1, 2, 3]`
- [ ] List concatenation `||`
- [ ] List indexing `list[index]`

### Struct Operators
- [ ] Struct field access `.field`
- [ ] Struct construction

### Pattern Matching
- [ ] SIMILAR TO operator
- [ ] DuckDB regex operators

---

## 📋 Phase 7: DuckDB Performance Features

### Parallel Query Execution
- [ ] Thread configuration (if exposed via SQL)
- [ ] Memory configuration

### Indexes
- [ ] CREATE INDEX (if different from postgres)
- [ ] ART indexes (DuckDB-specific)

### Statistics
- [ ] ANALYZE table
- [ ] Statistics views

---

## 📋 Phase 8: DuckDB Extensions

### Extensions System
- [ ] INSTALL extension via `install_extension(name)`
- [ ] LOAD extension via `load_extension(name)`

### Common Extensions
- [ ] httpfs for remote files
- [ ] parquet (built-in)
- [ ] json (built-in)
- [ ] Excel extension

### Extension-Specific Features
- [ ] Features that require extensions
- [ ] Extension detection/loading

---

## 📋 Phase 9: DuckDB Pragma Statements

### Configuration
- [ ] SET statements
- [ ] PRAGMA statements
- [ ] Configuration options

### Information
- [ ] DESCRIBE table
- [ ] SHOW tables
- [ ] SHOW functions

---

## 📋 Phase 10: DuckDB Type System

### DuckDB-Specific Types
- [ ] LIST type
- [ ] STRUCT type
- [ ] MAP type
- [ ] UNION type
- [ ] ENUM type
- [ ] BIT type
- [ ] BLOB type
- [ ] INTERVAL type

### Type Casting
- [ ] DuckDB cast syntax
- [ ] Type inference
- [ ] Implicit conversions

---

## Key Differences from PostgreSQL

### Syntax Differences
- DuckDB uses `$1, $2` parameter style by default (same as postgres prepared statements)
- Different type names (e.g., `TINYINT`, `UTINYINT`)
- Star extensions (EXCLUDE, REPLACE)
- Different string functions in some cases

### Feature Differences
- DuckDB has built-in Parquet/CSV reading
- DuckDB has LIST and STRUCT types
- DuckDB has different performance tuning options
- DuckDB is columnar, affects some optimization strategies

### Rendering Differences
- Some functions have different names
- Some syntax sugar differs
- Type casting syntax may differ
- Identifier quoting rules

---

## Implementation Strategy

1. **Start after Postgres Phase 6 complete**
   - Reuse all core infrastructure
   - Focus on DuckDB-specific features
   - Maintain test parity with postgres

2. **Incremental approach**
   - Phase 1: Basic setup and core features
   - Phase 2: Star extensions (highest value DuckDB feature)
   - Phase 3: File I/O (second highest value)
   - Remaining phases as needed

3. **Code reuse**
   - Share `vw/core/` completely
   - Share most of `vw/postgres/` logic
   - Only override what's different in `vw/duckdb/`

4. **Testing strategy**
   - Port all postgres tests
   - Add DuckDB-specific tests
   - Use same test patterns (sql() utility, etc.)

---

## Current Status Summary

**Completed:**
- None yet ❌

**Blocked by:**
- Postgres Phase 2: Operators & Expressions
- Postgres Phase 3: Aggregate & Window Functions
- Postgres Phase 4: Joins
- Postgres Phase 6: Parameters & Rendering

**Estimated Start:**
- After ~40% of postgres features complete
- Roughly after postgres reaches Phase 6

**Total Progress:** 0% complete (waiting on postgres)
