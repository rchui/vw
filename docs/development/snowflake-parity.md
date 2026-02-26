# Snowflake Parity Roadmap

Feature parity tracking for `vw/snowflake/` implementation.

**Status:** 🟡 In Progress
**Current Phase:** Phase 1 complete — core infrastructure and rendering
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

Snowflake inherits from `vw/core/` only — not from `vw/postgres/`. This keeps dialects independent and avoids inheriting PostgreSQL-specific behaviour that doesn't apply to Snowflake.

**Inherited from core:**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
- Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- Set operations (UNION, INTERSECT, EXCEPT)
- CTEs (Common Table Expressions)
- Subqueries and conditional expressions
- ANSI scalar functions (COALESCE, NULLIF, GREATEST, LEAST, etc.)
- Parameters and rendering

**Snowflake-Specific (new implementation required):**
- Parameter style: `%s` (pyformat — Snowflake Python connector default)
- Identifier quoting: double quotes, case-insensitive by default
- VARIANT type (JSON/semi-structured data)
- FLATTEN function for semi-structured data
- Time travel queries (AT/BEFORE)
- QUALIFY clause (post-window filtering)
- Table functions (GENERATOR, LATERAL FLATTEN)
- MATCH_RECOGNIZE for pattern matching

**Testing approach:** Render-only (no live Snowflake account required). All tests verify SQL string output, not execution results.

---

## Prerequisites (Inherited from Core)

These features from `vw/core/` are automatically available in Snowflake:

### ✅ Core Features (inherited)
- ✅ Query building (SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT)
- ✅ Operators and expressions (comparison, arithmetic, logical, pattern matching, NULL checks)
- ✅ Aggregate and window functions (COUNT, SUM, AVG, MIN, MAX, ROW_NUMBER, RANK, etc.)
- ✅ Joins (INNER, LEFT, RIGHT, FULL, CROSS), subqueries, CTEs, set operations
- ✅ ANSI scalar functions (COALESCE, NULLIF, GREATEST, LEAST, date/time functions)
- ✅ Parameters, literals, rendering infrastructure

---

## ✅ Phase 1: Core Snowflake Implementation

**Status:** ✅ Complete

### Infrastructure Setup
- [x] Create `vw/snowflake/` directory
- [x] Create `vw/snowflake/base.py` — `Expression` and `RowSet` inheriting from `CoreExpression`/`CoreRowSet`
- [x] Create `vw/snowflake/render.py` — Snowflake SQL rendering (inherits from core, not postgres)
- [x] Create `vw/snowflake/states.py` — Snowflake-specific states (placeholder for later phases)
- [x] Create `vw/snowflake/types.py` — Snowflake type system (VARIANT, OBJECT, ARRAY, NUMBER, TIMESTAMP_NTZ/LTZ/TZ)
- [x] Create `vw/snowflake/raw.py` — raw SQL escape hatches (`raw.expr`, `raw.rowset`, `raw.func`)
- [x] Create `vw/snowflake/public.py` — factory functions wired to Snowflake Expression/RowSet
- [x] Create `vw/snowflake/__init__.py` — public exports
- [x] Create `tests/snowflake/` directory structure

### Snowflake-Specific Rendering
- [x] Parameter style: `%(name)s` (PYFORMAT — Snowflake Python connector default)
- [x] Cast syntax: `CAST(expr AS type)` (not PostgreSQL's `::` shorthand)
- [x] `DISTINCT ON` raises `RenderError` (not supported in Snowflake)
- [x] Statement modifiers (FOR UPDATE etc.) raise `RenderError`
- [x] ILIKE supported (Snowflake supports case-insensitive LIKE)
- [x] LATERAL joins supported
- [x] Render `TRUE`/`FALSE` for boolean literals

### Public API (factory functions in `public.py`)
- [x] `ref()`, `col()`, `param()`, `lit()` — column/table/value factories
- [x] `when()` — CASE WHEN builder
- [x] `exists()` — EXISTS subquery
- [x] `values()` — VALUES clause
- [x] `cte()` — Common Table Expressions
- [x] `rollup()`, `cube()`, `grouping_sets()` — grouping constructs
- [x] `interval()` — INTERVAL literals
- [x] `render()` — render query to SQL string + params dict
- [x] `F` — function namespace (inherits `CoreFunctions`)
- [x] `raw` — raw SQL escape hatch
- [x] `T` — type shorthand namespace

### Testing
- [x] `tests/snowflake/test_render.py` — unit tests per render function (86 tests)
- [x] `tests/snowflake/integration/test_queries.py` — end-to-end rendering tests (45 tests)
- [x] Verify `%(name)s` parameter style
- [x] Verify `CAST(expr AS type)` syntax (no `::`)
- [x] Verify all inherited ANSI SQL features produce correct SQL

### Key Rendering Differences from PostgreSQL

| Feature | PostgreSQL | Snowflake |
|---|---|---|
| Parameters | `$name` | `%(name)s` |
| Cast | `expr::type` | `expr::type` (same) |
| DISTINCT ON | supported | raises RenderError |
| FOR UPDATE | supported | raises RenderError |
| NOW() | supported | not in Phase 1 (use CURRENT_TIMESTAMP) |
| DATE_TRUNC | supported | not in Phase 1 |
| ILIKE | supported | supported |
| LATERAL | supported | supported |

---

---

## 📋 Phase 2: VARIANT Type and Semi-Structured Data

**Status:** ❌ Not Started
**Priority:** HIGH - Core Snowflake differentiator

### VARIANT Type
- [ ] VARIANT type support for JSON/semi-structured data
- [ ] Type casting to/from VARIANT
- [ ] OBJECT type (JSON objects)
- [ ] ARRAY type (JSON arrays)

### Semi-Structured Data Access
- [ ] Bracket notation: `col("data")["field"]`
- [ ] Dot notation: `col("data").field` (if possible)
- [ ] Path traversal: `col("data")["users"][0]["name"]`
- [ ] Safe navigation operators

### Data Structures Needed
- [ ] VariantType dataclass
- [ ] PathAccess dataclass for nested field access
- [ ] VARIANT literal support

### Examples
```python
# Access VARIANT fields
col("json_data")["user"]["name"]
col("json_data")["items"][0]

# Cast to VARIANT
col("json_string").cast(T.variant())
```

---

## 📋 Phase 3: FLATTEN and Table Functions

**Status:** ❌ Not Started
**Priority:** HIGH - Essential for semi-structured data

### FLATTEN Function
- [ ] `FLATTEN(input => col, path => "path")` for unnesting arrays
- [ ] Via `F.flatten(col("array_field"), path="items")`
- [ ] LATERAL FLATTEN joins
- [ ] Recursive FLATTEN

### Table Functions
- [ ] GENERATOR for creating test data
- [ ] TABLE for lateral joins with table functions
- [ ] RESULT_SCAN for querying previous results

### Data Structures Needed
- [ ] Flatten dataclass (extends table function)
- [ ] TableFunction base class
- [ ] Generator dataclass

### Examples
```python
# FLATTEN array
source("data").join.lateral(
    F.flatten(col("items")).alias("f"),
    on=[]
)
# Renders: SELECT * FROM data, LATERAL FLATTEN(input => items) f

# GENERATOR
F.generator(rowcount=1000)
```

---

## 📋 Phase 4: Time Travel

**Status:** ❌ Not Started
**Priority:** MEDIUM - Unique Snowflake feature

### Time Travel Queries
- [ ] AT (TIMESTAMP => expr)
- [ ] AT (OFFSET => expr)
- [ ] AT (STATEMENT => expr)
- [ ] BEFORE (STATEMENT => expr)
- [ ] Via `.at(timestamp=...)` or `.at(offset=...)` or `.before(...)`

### Data Structures Needed
- [ ] TimeTravel dataclass
- [ ] TimeTravelType enum (TIMESTAMP, OFFSET, STATEMENT)

### Examples
```python
# Query as of timestamp
source("orders").at(timestamp="2024-01-01 00:00:00")
# Renders: SELECT * FROM orders AT(TIMESTAMP => '2024-01-01 00:00:00')

# Query as of offset
source("orders").at(offset="-5 minutes")
# Renders: SELECT * FROM orders AT(OFFSET => '-5 minutes')

# Query before statement
source("orders").before(statement="query_id_123")
```

---

## 📋 Phase 5: QUALIFY Clause

**Status:** ❌ Not Started
**Priority:** MEDIUM - Convenient window function filtering

**Note:** QUALIFY is already implemented in `vw/duckdb`. If/when it makes sense to move QUALIFY state to `vw/core`, Snowflake can inherit it for free. Until then, implement it independently in `vw/snowflake`.

### QUALIFY Clause
- [ ] Post-window function filtering via `.qualify(condition)`
- [ ] QUALIFY with window functions
- [ ] Multiple QUALIFY conditions (AND combined)

### Data Structures Needed
- [ ] `qualify_conditions` field on `Statement` state (or shared via core)

### Examples
```python
# Filter after window function
ref("sales").select(
    col("product"),
    col("amount"),
    F.rank().over(order_by=[col("amount").desc()]).alias("rank")
).qualify(col("rank") <= 10)
# Renders: SELECT product, amount, RANK() OVER (ORDER BY amount DESC) AS rank
#          FROM sales
#          QUALIFY rank <= 10
```

---

## 📋 Phase 6: Snowflake-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### Semi-Structured Functions
- [ ] `PARSE_JSON()` - parse JSON string
- [ ] `TRY_PARSE_JSON()` - safe JSON parsing
- [ ] `OBJECT_CONSTRUCT()` - build JSON object
- [ ] `ARRAY_CONSTRUCT()` - build JSON array
- [ ] `GET_PATH()` - extract path from VARIANT

### String Functions
- [ ] `SPLIT()` - split string to array
- [ ] `SPLIT_TO_TABLE()` - split and unnest

### Date/Time Functions
- [ ] Snowflake-specific date functions
- [ ] Time zone conversions
- [ ] Date arithmetic variations

---

## 📋 Phase 7: MATCH_RECOGNIZE

**Status:** ❌ Not Started
**Priority:** LOW - Advanced pattern matching

### Pattern Matching
- [ ] MATCH_RECOGNIZE clause for pattern matching
- [ ] Row pattern syntax
- [ ] Pattern variables

### Examples
```python
# Pattern matching (future API)
source("events").match_recognize(
    pattern="A B+ C",
    measures={"count": F.count(col("*"))},
    define={"B": col("status") == "processing"}
)
```

---

## 📋 Phase 8: Snowflake Stages and File Formats

**Status:** ❌ Not Started
**Priority:** LOW

### Stage Operations
- [ ] Reference external stages: `@stage_name/path`
- [ ] COPY INTO from stage
- [ ] Query files in stages

### File Formats
- [ ] CSV, JSON, Parquet, Avro, ORC
- [ ] Custom file format references

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Parameter style: Named (`:name`) or positional (`?`)
- ⚠️ Identifiers: Case-insensitive by default (stored uppercase unless quoted)
- ⚠️ VARIANT/OBJECT/ARRAY types for semi-structured data
- ⚠️ FLATTEN function for unnesting
- ⚠️ QUALIFY clause for window function filtering

### Feature Differences
- ➕ VARIANT type for JSON/semi-structured data
- ➕ Time travel (AT/BEFORE)
- ➕ FLATTEN for unnesting arrays
- ➕ QUALIFY clause
- ➕ MATCH_RECOGNIZE pattern matching
- ➕ Cloud-native features (stages, external tables)
- ➖ No PostgreSQL-specific features (ILIKE, array operators)
- ➖ Different full-text search

---

## Implementation Strategy

### 1. Inherit from Core Only
- `vw/snowflake/base.py` inherits `Expression`/`RowSet` from `vw/core/base.py`
- `vw/snowflake/render.py` inherits from `vw/core/render.py`
- No dependency on `vw/postgres/` anywhere

### 2. Incremental Approach
- **Phase 1:** Core infrastructure and rendering (all ANSI SQL works) ← start here
- **Phase 2:** VARIANT type and semi-structured data 🌟
- **Phase 3:** FLATTEN and table functions 🌟
- **Phase 4-8:** Additional Snowflake features as needed

### 3. Testing Strategy
- Render-only tests — no live Snowflake account required
- Port relevant postgres render tests, adapting for `%s` parameter style
- Add Snowflake-specific tests for each new feature

---

## Priority Matrix

### HIGH Priority (Core Snowflake Value)
1. ⭐ Phase 1: Core infrastructure (all ANSI SQL, correct parameter style)
2. ⭐ VARIANT type and semi-structured data access
3. ⭐ FLATTEN function for unnesting

### MEDIUM Priority (Common Use Cases)
4. Time travel queries (AT/BEFORE)
5. QUALIFY clause
6. Snowflake-specific functions

### LOW Priority (Nice to Have)
7. MATCH_RECOGNIZE
8. Stage operations and file formats

**Total Progress:** Phase 1 complete (8 phases total)
