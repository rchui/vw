# SQLite Parity Roadmap

Feature parity tracking for `vw/sqlite/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

SQLite implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, adapting to SQLite's lightweight feature set and syntax.

**Shared with PostgreSQL (via inheritance):**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.) - SQLite 3.25+
- Joins (INNER, LEFT, RIGHT via workaround, FULL via workaround, CROSS)
- Set operations (UNION, INTERSECT, EXCEPT)
- CTEs (Common Table Expressions)
- Subqueries and conditional expressions
- Parameters and rendering

**SQLite-Specific (new implementation required):**
- Double quote or backtick identifier quoting
- Parameter style (`:name`, `?`, `?NNN`, `$name`, `@name`)
- Limited type system (TEXT, INTEGER, REAL, BLOB, NULL)
- No RIGHT JOIN or FULL OUTER JOIN (require workarounds)
- AUTOINCREMENT vs PostgreSQL SERIAL
- SQLite-specific functions (GROUP_CONCAT, etc.)
- PRAGMA statements
- Virtual tables and FTS5

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in SQLite:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions (SQLite 3.25+)
- ✅ Joins (with workarounds for RIGHT/FULL), subqueries, CTEs
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core SQLite Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/sqlite/` directory
- [ ] Create `vw/sqlite/base.py` with SQLite-specific classes
- [ ] Create `vw/sqlite/public.py` for public API
- [ ] Create `vw/sqlite/render.py` for SQLite SQL rendering
- [ ] Set up `vw/sqlite/__init__.py` exports
- [ ] Create `tests/sqlite/` directory structure

### SQLite-Specific Rendering
- [ ] Handle identifier quoting (double quotes or backticks)
- [ ] Parameter style (`:name`, `?`, `$name`, etc.)
- [ ] Type name differences (simplified type system)
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features

### Testing Setup
- [ ] Set up test database fixtures (in-memory SQLite)
- [ ] Port relevant postgres tests to sqlite
- [ ] Create SQLite-specific test utilities
- [ ] Handle missing features (RIGHT JOIN, FULL OUTER JOIN)
- [ ] Verify inherited features work correctly

---

## 📋 Phase 2: Join Workarounds

**Status:** ❌ Not Started
**Priority:** MEDIUM

### RIGHT JOIN Workaround
- [ ] Implement RIGHT JOIN as reversed LEFT JOIN
- [ ] Automatic rewriting of RIGHT JOIN

### FULL OUTER JOIN Workaround
- [ ] Implement FULL OUTER JOIN as UNION of LEFT and RIGHT (anti-join)
- [ ] Automatic rewriting when requested

### Examples
```python
# RIGHT JOIN (rewritten as LEFT JOIN)
a.join.right(b, on=[...])
# Renders: SELECT * FROM b LEFT JOIN a ON ...

# FULL OUTER JOIN (rewritten as UNION)
a.join.full_outer(b, on=[...])
# Renders: SELECT * FROM a LEFT JOIN b ON ...
#          UNION
#          SELECT * FROM b LEFT JOIN a ON ... WHERE a.key IS NULL
```

---

## 📋 Phase 3: SQLite-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM

### String Functions
- [ ] `GROUP_CONCAT()` - concatenate grouped values
- [ ] `SUBSTR()` - substring extraction
- [ ] `INSTR()` - find substring position
- [ ] `REPLACE()` - replace substring
- [ ] `TRIM()`, `LTRIM()`, `RTRIM()`

### Date/Time Functions
- [ ] `DATE()`, `TIME()`, `DATETIME()` - date/time constructors
- [ ] `JULIANDAY()` - convert to Julian day
- [ ] `STRFTIME()` - format date/time
- [ ] Date arithmetic with modifiers

### Aggregate Functions
- [ ] `GROUP_CONCAT()` with separator

### Math Functions
- [ ] Basic math functions (ABS, ROUND, etc.)

### Examples
```python
# GROUP_CONCAT
F.group_concat(col("name"), separator=", ")
# Renders: GROUP_CONCAT(name, ', ')

# STRFTIME
col("created_at").dt.strftime("%Y-%m-%d")
```

---

## 📋 Phase 4: SQLite Type System

**Status:** ❌ Not Started
**Priority:** LOW

### Type Affinity
- [ ] TEXT affinity
- [ ] INTEGER affinity
- [ ] REAL affinity
- [ ] BLOB affinity
- [ ] NULL affinity

### Type Handling
- [ ] Map PostgreSQL types to SQLite affinities
- [ ] Handle type casting appropriately

---

## 📋 Phase 5: PRAGMA Statements

**Status:** ❌ Not Started
**Priority:** LOW

### Common PRAGMA
- [ ] PRAGMA foreign_keys
- [ ] PRAGMA journal_mode
- [ ] PRAGMA synchronous
- [ ] Via utility functions

### Examples
```python
# Execute PRAGMA (utility function)
pragma("foreign_keys", "ON")
# Executes: PRAGMA foreign_keys = ON
```

---

## 📋 Phase 6: Virtual Tables and FTS

**Status:** ❌ Not Started
**Priority:** LOW

### FTS5 Full-Text Search
- [ ] Create FTS5 tables
- [ ] FTS5 MATCH operator
- [ ] FTS5 functions (rank, highlight)

### Virtual Tables
- [ ] Reference virtual tables
- [ ] Virtual table-specific operations

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Parameter style: Multiple styles (`:name`, `?`, `$name`, `@name`)
- ⚠️ Simplified type system (TEXT, INTEGER, REAL, BLOB, NULL)
- ⚠️ Some function names differ (SUBSTR vs SUBSTRING)

### Feature Differences
- ➕ GROUP_CONCAT aggregate function
- ➕ Lightweight and embedded
- ➕ FTS5 full-text search
- ➕ Virtual tables
- ➖ No RIGHT JOIN (can be worked around)
- ➖ No FULL OUTER JOIN (can be worked around)
- ➖ No ILIKE operator (use LOWER + LIKE)
- ➖ No array types or operators
- ➖ Limited window functions (SQLite 3.25+)
- ➖ No CTEs with UPDATE/DELETE (only SELECT)
- ➖ More limited date/time functionality
- ➖ No schemas (single database)

### Limitations
- No stored procedures or functions
- Limited ALTER TABLE support
- No user-defined functions in pure SQL

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure
- Adapt to SQLite's simpler feature set
- Implement workarounds for missing features

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering
- **Phase 2:** Join workarounds for RIGHT/FULL OUTER
- **Phase 3:** SQLite-specific functions
- **Phase 4-6:** Additional SQLite features as needed

---

## Priority Matrix

### MEDIUM Priority
1. Basic rendering (parameters, types)
2. Join workarounds (RIGHT, FULL OUTER)
3. SQLite-specific functions (GROUP_CONCAT, date functions)

### LOW Priority
4. Type system handling
5. PRAGMA statements
6. Virtual tables and FTS5

**Total Progress:** 0% complete (waiting on postgres infrastructure)
