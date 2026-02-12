# Snowflake Parity Roadmap

Feature parity tracking for `vw/snowflake/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

Snowflake implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, focusing on Snowflake-specific features and cloud data warehouse capabilities.

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

**Snowflake-Specific (new implementation required):**
- VARIANT type (JSON/semi-structured data)
- FLATTEN function for semi-structured data
- Time travel queries (AT/BEFORE)
- QUALIFY clause (post-window filtering)
- Table functions (GENERATOR, LATERAL FLATTEN)
- Snowflake-specific stages and file formats
- MATCH_RECOGNIZE for pattern matching
- Snowflake naming conventions (case-insensitive by default)

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in Snowflake:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions
- ✅ Joins, subqueries, CTEs
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core Snowflake Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/snowflake/` directory
- [ ] Create `vw/snowflake/base.py` with Snowflake-specific classes
- [ ] Create `vw/snowflake/public.py` for public API
- [ ] Create `vw/snowflake/render.py` for Snowflake SQL rendering
- [ ] Set up `vw/snowflake/__init__.py` exports
- [ ] Create `tests/snowflake/` directory structure

### Snowflake-Specific Rendering
- [ ] Handle Snowflake identifier quoting (double quotes, but case-insensitive by default)
- [ ] Parameter style (named parameters: `:name` or `?`)
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features that don't apply

### Testing Setup
- [ ] Set up test database fixtures (Snowflake test account)
- [ ] Port relevant postgres tests to snowflake
- [ ] Create Snowflake-specific test utilities
- [ ] Verify all inherited features work correctly

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

### QUALIFY Clause
- [ ] Post-window function filtering via `.qualify(condition)`
- [ ] QUALIFY with window functions
- [ ] Multiple QUALIFY conditions (AND combined)

### Data Structures Needed
- [ ] Qualify dataclass added to Statement

### Examples
```python
# Filter after window function
source("sales").select(
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

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure from `vw/core/` and `vw/postgres/`
- Focus on Snowflake-specific features only

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering
- **Phase 2:** VARIANT type and semi-structured data 🌟 HIGH PRIORITY
- **Phase 3:** FLATTEN and table functions 🌟 HIGH PRIORITY
- **Phase 4-8:** Additional Snowflake features as needed

### 3. Code Reuse Strategy
- Inherit from postgres where possible
- Override identifier quoting and parameter style
- Add Snowflake-specific classes for VARIANT, FLATTEN, etc.

---

## Priority Matrix

### HIGH Priority (Core Snowflake Value)
1. ⭐ VARIANT type and semi-structured data access
2. ⭐ FLATTEN function for unnesting

### MEDIUM Priority (Common Use Cases)
3. Time travel queries
4. QUALIFY clause
5. Snowflake-specific functions

### LOW Priority (Nice to Have)
6. MATCH_RECOGNIZE
7. Advanced stage operations
8. File format operations

**Total Progress:** 0% complete (waiting on postgres infrastructure)
