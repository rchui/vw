# Oracle Parity Roadmap

Feature parity tracking for `vw/oracle/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

Oracle implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, adapting to Oracle's enterprise features and PL/SQL syntax.

**Shared with PostgreSQL (via inheritance):**
- Core query building (SELECT, WHERE, GROUP BY, etc.)
- Operators and expressions
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
- Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- Set operations (UNION, INTERSECT, EXCEPT/MINUS)
- CTEs (Common Table Expressions) - Oracle 11g R2+
- Subqueries and conditional expressions
- Parameters and rendering

**Oracle-Specific (new implementation required):**
- ROWNUM for row limiting (Oracle < 12c)
- FETCH FIRST (Oracle 12c+)
- DUAL table
- Double quote identifier quoting (but identifiers uppercase by default)
- Parameter style (`:name` for named, `:1` for positional)
- CONNECT BY for hierarchical queries
- MERGE statement (upsert)
- Oracle-specific functions (LISTAGG, NVL, DECODE, etc.)
- Analytic functions (Oracle's window functions)
- Oracle date literals and functions
- (+) outer join syntax (legacy, not recommended)

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in Oracle:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions
- ✅ Joins, subqueries, CTEs (Oracle 11g R2+)
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core Oracle Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/oracle/` directory
- [ ] Create `vw/oracle/base.py` with Oracle-specific classes
- [ ] Create `vw/oracle/public.py` for public API
- [ ] Create `vw/oracle/render.py` for Oracle SQL rendering
- [ ] Set up `vw/oracle/__init__.py` exports
- [ ] Create `tests/oracle/` directory structure

### Oracle-Specific Rendering
- [ ] Handle identifier quoting (double quotes, uppercase by default)
- [ ] Parameter style (`:name` for named, `:1`, `:2` for positional)
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features
- [ ] Handle DUAL table automatically where needed

### Testing Setup
- [ ] Set up test database fixtures (Oracle test instance)
- [ ] Port relevant postgres tests to oracle
- [ ] Create Oracle-specific test utilities
- [ ] Verify inherited features work correctly

---

## 📋 Phase 2: Row Limiting (ROWNUM vs FETCH FIRST)

**Status:** ❌ Not Started
**Priority:** HIGH - Different approaches for different Oracle versions

### ROWNUM (Oracle < 12c)
- [ ] Use ROWNUM in WHERE clause for limiting
- [ ] Subquery wrapper for OFFSET + LIMIT
- [ ] Via `.limit(n).offset(m)` with automatic ROWNUM rewriting

### FETCH FIRST (Oracle 12c+)
- [ ] `FETCH FIRST n ROWS ONLY`
- [ ] `OFFSET n ROWS FETCH NEXT m ROWS ONLY`
- [ ] Via `.fetch(n)` and `.offset(n).fetch(m)`

### Data Structures Needed
- [ ] OracleVersion detection/configuration
- [ ] ROWNUM rewriting logic

### Examples
```python
# Oracle 11g (ROWNUM)
source("users").select(col("name")).limit(10)
# Renders: SELECT * FROM (SELECT name FROM users) WHERE ROWNUM <= 10

# Oracle 12c+ (FETCH FIRST)
source("users").select(col("name")).fetch(10)
# Renders: SELECT name FROM users FETCH FIRST 10 ROWS ONLY

# With OFFSET (Oracle 11g - complex subquery)
source("users").limit(10).offset(20)
# Renders: Complex nested query with ROWNUM

# With OFFSET (Oracle 12c+)
source("users").offset(20).fetch(10)
# Renders: SELECT * FROM users OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
```

---

## 📋 Phase 3: Oracle-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM

### String Functions
- [ ] `LISTAGG()` - aggregate strings with separator (within group)
- [ ] `NVL()` - null value substitution (like COALESCE)
- [ ] `NVL2()` - conditional null value substitution
- [ ] `DECODE()` - multi-way branch (like CASE)
- [ ] `INSTR()` - find substring position
- [ ] `SUBSTR()` - substring extraction

### Date/Time Functions
- [ ] `SYSDATE` - current timestamp
- [ ] `SYSTIMESTAMP` - current timestamp with fractional seconds
- [ ] `TO_DATE()` - parse date string
- [ ] `TO_CHAR()` - format date/value as string
- [ ] `ADD_MONTHS()` - add months to date
- [ ] `MONTHS_BETWEEN()` - difference in months
- [ ] `TRUNC()` - truncate date to unit
- [ ] `EXTRACT()` - extract date part

### Null Handling
- [ ] `NVL()` - replace NULL (like COALESCE with 2 args)
- [ ] `NVL2()` - conditional on NULL
- [ ] `NULLIF()` - return NULL if equal

### Aggregate Functions
- [ ] `LISTAGG()` with WITHIN GROUP (ORDER BY)

### Examples
```python
# LISTAGG
F.listagg(col("name"), ", ", order_by=[col("name").asc()])
# Renders: LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name ASC)

# NVL
F.nvl(col("email"), "unknown@example.com")
# Renders: NVL(email, 'unknown@example.com')

# DECODE
F.decode(col("status"), "A", "Active", "I", "Inactive", "Unknown")
# Renders: DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')
```

---

## 📋 Phase 4: CONNECT BY (Hierarchical Queries)

**Status:** ❌ Not Started
**Priority:** MEDIUM - Unique Oracle feature

### CONNECT BY Clause
- [ ] CONNECT BY for hierarchical/recursive queries
- [ ] START WITH clause
- [ ] PRIOR operator
- [ ] LEVEL pseudo-column
- [ ] Via `.connect_by(condition, start_with=None)`

### Data Structures Needed
- [ ] ConnectBy dataclass
- [ ] Prior operator wrapper

### Examples
```python
# Hierarchical query
source("employees").select(
    col("employee_id"),
    col("manager_id"),
    col("name"),
    F.level().alias("level")
).connect_by(
    prior=col("employee_id") == col("manager_id"),
    start_with=col("manager_id").is_null()
)
# Renders: SELECT employee_id, manager_id, name, LEVEL
#          FROM employees
#          START WITH manager_id IS NULL
#          CONNECT BY PRIOR employee_id = manager_id
```

---

## 📋 Phase 5: MERGE Statement

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### MERGE Statement
- [ ] MERGE for upsert operations
- [ ] WHEN MATCHED / WHEN NOT MATCHED
- [ ] ON condition
- [ ] Via `merge_into(target).using(source).on(...)`

### Examples
```python
# MERGE
merge_into("users").using(
    source("staging_users"),
    on=col("users.id") == col("staging_users.id")
).when_matched(
    update_set={"name": col("staging_users.name")}
).when_not_matched(
    insert={"id": col("staging_users.id"), "name": col("staging_users.name")}
)
```

---

## 📋 Phase 6: DUAL Table

**Status:** ❌ Not Started
**Priority:** LOW - Automatically handled

### DUAL Table
- [ ] Automatically use DUAL for queries without FROM
- [ ] Handle SELECT without FROM clause

### Examples
```python
# SELECT without table (uses DUAL automatically)
select(lit(1).alias("one"))
# Renders: SELECT 1 AS one FROM DUAL
```

---

## 📋 Phase 7: Oracle Analytic Functions

**Status:** ❌ Not Started
**Priority:** LOW - Most inherited from PostgreSQL

### Oracle-Specific Analytic Functions
- [ ] `RATIO_TO_REPORT()` - ratio to sum
- [ ] `CUME_DIST()` - cumulative distribution (inherited)
- [ ] `PERCENT_RANK()` - percentile rank (inherited)
- [ ] Oracle-specific window frame syntax

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Parameter style: Named (`:name`) or positional (`:1`, `:2`)
- ⚠️ Identifiers: Uppercase by default unless quoted
- ⚠️ ROWNUM for row limiting (Oracle < 12c)
- ⚠️ FETCH FIRST available in Oracle 12c+
- ⚠️ Some function names differ (NVL vs COALESCE, DECODE vs CASE)
- ⚠️ MINUS instead of EXCEPT for set difference

### Feature Differences
- ➕ CONNECT BY for hierarchical queries
- ➕ MERGE statement
- ➕ LISTAGG aggregate function
- ➕ NVL, NVL2, DECODE functions
- ➕ DUAL table
- ➕ (+) outer join syntax (legacy, not recommended)
- ➖ No ILIKE operator (use LOWER + LIKE)
- ➖ No array types or operators
- ➖ Different full-text search (Oracle Text)
- ➖ CTEs only in Oracle 11g R2+

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure
- Adapt to Oracle syntax and features

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering
- **Phase 2:** Row limiting (ROWNUM vs FETCH) 🌟 HIGH PRIORITY
- **Phase 3-7:** Additional Oracle features as needed

---

## Priority Matrix

### HIGH Priority
1. ⭐ Row limiting (ROWNUM for <12c, FETCH FIRST for 12c+)

### MEDIUM Priority
2. Oracle-specific functions (LISTAGG, NVL, DECODE)
3. CONNECT BY hierarchical queries
4. MERGE statement

### LOW Priority
5. DUAL table handling (automatic)
6. Oracle-specific analytic functions
7. Legacy outer join syntax

**Total Progress:** 0% complete (waiting on postgres infrastructure)
