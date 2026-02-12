# MySQL Parity Roadmap

Feature parity tracking for `vw/mysql/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

MySQL implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, adapting to MySQL's syntax and feature set.

**Shared with PostgreSQL (via inheritance):**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.) - MySQL 8.0+
- Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- Set operations (UNION, INTERSECT, EXCEPT) - INTERSECT/EXCEPT in MySQL 8.0.31+
- CTEs (Common Table Expressions) - MySQL 8.0+
- Subqueries and conditional expressions
- Parameters and rendering

**MySQL-Specific (new implementation required):**
- LIMIT with OFFSET syntax (`LIMIT offset, count`)
- Backtick identifier quoting
- MySQL-specific functions (GROUP_CONCAT, etc.)
- ON DUPLICATE KEY UPDATE
- REPLACE INTO
- MySQL-specific index hints (FORCE INDEX, USE INDEX, IGNORE INDEX)
- STRAIGHT_JOIN hint
- MySQL date/time functions

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in MySQL:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions (MySQL 8.0+)
- ✅ Joins, subqueries, CTEs (MySQL 8.0+)
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core MySQL Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/mysql/` directory
- [ ] Create `vw/mysql/base.py` with MySQL-specific classes
- [ ] Create `vw/mysql/public.py` for public API
- [ ] Create `vw/mysql/render.py` for MySQL SQL rendering
- [ ] Set up `vw/mysql/__init__.py` exports
- [ ] Create `tests/mysql/` directory structure

### MySQL-Specific Rendering
- [ ] Handle backtick identifier quoting
- [ ] Parameter style (`?` or `%s` depending on driver)
- [ ] LIMIT syntax: `LIMIT count` or `LIMIT offset, count`
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features

### Testing Setup
- [ ] Set up test database fixtures (MySQL test instance)
- [ ] Port relevant postgres tests to mysql
- [ ] Create MySQL-specific test utilities
- [ ] Verify inherited features work correctly

---

## 📋 Phase 2: MySQL-Specific LIMIT Syntax

**Status:** ❌ Not Started
**Priority:** MEDIUM

### LIMIT Variations
- [ ] `LIMIT count` (standard)
- [ ] `LIMIT offset, count` (MySQL-specific)
- [ ] `LIMIT count OFFSET offset` (SQL standard, MySQL 8.0+)

### Examples
```python
# LIMIT with offset
source("users").select(col("name")).limit(10).offset(20)
# Renders: SELECT name FROM users LIMIT 20, 10
# Or: SELECT name FROM users LIMIT 10 OFFSET 20 (MySQL 8.0+)
```

---

## 📋 Phase 3: MySQL-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM

### String Functions
- [ ] `GROUP_CONCAT()` - concatenate grouped values
- [ ] `CONCAT_WS()` - concatenate with separator
- [ ] `SUBSTRING_INDEX()` - extract substring

### Date/Time Functions
- [ ] `NOW()` - current timestamp
- [ ] `CURDATE()` - current date
- [ ] `CURTIME()` - current time
- [ ] `DATE_FORMAT()` - format date
- [ ] `STR_TO_DATE()` - parse date string
- [ ] `TIMESTAMPDIFF()` - date difference
- [ ] `DATE_ADD()` / `DATE_SUB()` - date arithmetic

### Aggregate Functions
- [ ] `GROUP_CONCAT()` with ORDER BY and SEPARATOR

### Examples
```python
# GROUP_CONCAT
F.group_concat(col("name"), separator=", ", order_by=[col("name").asc()])
# Renders: GROUP_CONCAT(name ORDER BY name ASC SEPARATOR ', ')

# DATE_FORMAT
col("created_at").dt.date_format("%Y-%m-%d")
```

---

## 📋 Phase 4: INSERT Extensions

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### ON DUPLICATE KEY UPDATE
- [ ] `INSERT ... ON DUPLICATE KEY UPDATE`
- [ ] Via `.on_duplicate_key_update(**updates)`

### REPLACE INTO
- [ ] `REPLACE INTO` statement
- [ ] Via `replace_into(table).values(...)`

### Examples
```python
# ON DUPLICATE KEY UPDATE
insert_into("users").values(
    id=1, name="Alice", email="alice@example.com"
).on_duplicate_key_update(
    name=VALUES(col("name")),
    email=VALUES(col("email"))
)
# Renders: INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')
#          ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)
```

---

## 📋 Phase 5: Index Hints

**Status:** ❌ Not Started
**Priority:** LOW

### Index Hints
- [ ] FORCE INDEX
- [ ] USE INDEX
- [ ] IGNORE INDEX
- [ ] Via `.index_hint(type, *indexes)`

### Examples
```python
# Force index usage
source("users").index_hint("FORCE", "idx_email").where(col("email") == "test@example.com")
# Renders: SELECT * FROM users FORCE INDEX (idx_email) WHERE email = 'test@example.com'
```

---

## 📋 Phase 6: MySQL Query Hints

**Status:** ❌ Not Started
**Priority:** LOW

### Query Hints
- [ ] STRAIGHT_JOIN - force join order
- [ ] SQL_SMALL_RESULT / SQL_BIG_RESULT
- [ ] SQL_CALC_FOUND_ROWS (deprecated in MySQL 8.0.17)

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Identifier quoting: Backticks (\`) instead of double quotes
- ⚠️ Parameter style: `?` or `%s` depending on driver (not `$1`)
- ⚠️ LIMIT syntax: `LIMIT offset, count` (legacy) or `LIMIT count OFFSET offset`
- ⚠️ Some function names differ

### Feature Differences
- ➕ GROUP_CONCAT aggregate function
- ➕ ON DUPLICATE KEY UPDATE
- ➕ REPLACE INTO
- ➕ Index hints (FORCE INDEX, USE INDEX, IGNORE INDEX)
- ➖ No ILIKE operator (use LOWER + LIKE)
- ➖ Limited full-text search (different from PostgreSQL)
- ➖ No array types or operators
- ➖ Window functions only in MySQL 8.0+
- ➖ CTEs only in MySQL 8.0+
- ➖ INTERSECT/EXCEPT only in MySQL 8.0.31+

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure
- Adapt to MySQL syntax differences

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering (backticks, parameter style)
- **Phase 2-3:** MySQL-specific syntax and functions
- **Phase 4-6:** Advanced MySQL features as needed

---

## Priority Matrix

### MEDIUM Priority
1. MySQL-specific rendering (backticks, parameters, LIMIT)
2. MySQL-specific functions (GROUP_CONCAT, date functions)
3. INSERT extensions (ON DUPLICATE KEY UPDATE)

### LOW Priority
4. Index hints
5. Query hints

**Total Progress:** 0% complete (waiting on postgres infrastructure)
