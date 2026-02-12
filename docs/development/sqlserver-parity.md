# SQL Server Parity Roadmap

Feature parity tracking for `vw/sqlserver/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

SQL Server implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, adapting to SQL Server's T-SQL syntax and features.

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

**SQL Server-Specific (new implementation required):**
- TOP clause (with PERCENT and WITH TIES)
- OFFSET/FETCH (SQL Server 2012+)
- Square bracket identifier quoting
- T-SQL functions (STRING_AGG, CONCAT_WS, FORMAT, etc.)
- OUTPUT clause
- MERGE statement
- Table variables and temp tables
- TRY_CAST, TRY_CONVERT
- CROSS APPLY / OUTER APPLY
- FOR XML / FOR JSON

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in SQL Server:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions
- ✅ Joins, subqueries, CTEs
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core SQL Server Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/sqlserver/` directory
- [ ] Create `vw/sqlserver/base.py` with SQL Server-specific classes
- [ ] Create `vw/sqlserver/public.py` for public API
- [ ] Create `vw/sqlserver/render.py` for T-SQL rendering
- [ ] Set up `vw/sqlserver/__init__.py` exports
- [ ] Create `tests/sqlserver/` directory structure

### SQL Server-Specific Rendering
- [ ] Handle square bracket identifier quoting `[table].[column]`
- [ ] Parameter style (named: `@param` or positional)
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features

### Testing Setup
- [ ] Set up test database fixtures (SQL Server test instance)
- [ ] Port relevant postgres tests to sqlserver
- [ ] Create SQL Server-specific test utilities
- [ ] Verify inherited features work correctly

---

## 📋 Phase 2: TOP Clause

**Status:** ❌ Not Started
**Priority:** HIGH - SQL Server's primary row limiting mechanism

### TOP Clause
- [ ] `TOP n` - limit to n rows
- [ ] `TOP n PERCENT` - limit to n percent of rows
- [ ] `TOP n WITH TIES` - include ties for last value
- [ ] Via `.top(count, percent=False, with_ties=False)`

### Data Structures Needed
- [ ] TopClause dataclass

### Examples
```python
# TOP 10
source("users").select(col("name")).top(10)
# Renders: SELECT TOP 10 name FROM users

# TOP 10 PERCENT
source("users").select(col("name")).top(10, percent=True)
# Renders: SELECT TOP 10 PERCENT name FROM users

# TOP 10 WITH TIES
source("users").select(col("name")).order_by(col("score").desc()).top(10, with_ties=True)
# Renders: SELECT TOP 10 WITH TIES name FROM users ORDER BY score DESC
```

---

## 📋 Phase 3: OFFSET/FETCH (SQL Server 2012+)

**Status:** ❌ Not Started
**Priority:** MEDIUM

### OFFSET/FETCH Clause
- [ ] `OFFSET n ROWS FETCH NEXT m ROWS ONLY`
- [ ] Requires ORDER BY (SQL Server requirement)
- [ ] Via `.offset(n).fetch(m)` (inherited but needs validation)

### Examples
```python
# OFFSET/FETCH requires ORDER BY in SQL Server
source("users").order_by(col("id")).offset(10).fetch(20)
# Renders: SELECT * FROM users ORDER BY id OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY
```

---

## 📋 Phase 4: APPLY Operators

**Status:** ❌ Not Started
**Priority:** MEDIUM - SQL Server's version of LATERAL

### CROSS APPLY / OUTER APPLY
- [ ] CROSS APPLY (like INNER JOIN with table function)
- [ ] OUTER APPLY (like LEFT JOIN with table function)
- [ ] Via `.join.cross_apply(...)` and `.join.outer_apply(...)`

### Data Structures Needed
- [ ] ApplyType enum (CROSS, OUTER)
- [ ] Apply joins in join system

### Examples
```python
# CROSS APPLY
source("orders").join.cross_apply(
    F.string_split(col("items"), ",").alias("item"),
    on=[]
)
# Renders: SELECT * FROM orders CROSS APPLY STRING_SPLIT(items, ',') item
```

---

## 📋 Phase 5: SQL Server-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM

### String Functions
- [ ] `STRING_AGG()` - aggregate strings with separator
- [ ] `STRING_SPLIT()` - split string to table
- [ ] `CONCAT_WS()` - concatenate with separator
- [ ] `FORMAT()` - format values
- [ ] `TRIM()`, `LTRIM()`, `RTRIM()`

### Date/Time Functions
- [ ] `GETDATE()` - current timestamp
- [ ] `SYSDATETIME()` - high precision current timestamp
- [ ] `DATEADD()` - add interval to date
- [ ] `DATEDIFF()` - difference between dates
- [ ] `EOMONTH()` - end of month
- [ ] `DATEFROMPARTS()` - construct date
- [ ] `FORMAT()` - format date

### Type Conversion
- [ ] `TRY_CAST()` - safe casting
- [ ] `TRY_CONVERT()` - safe conversion
- [ ] `TRY_PARSE()` - safe parsing

### Aggregate Functions
- [ ] `STRING_AGG()` with WITHIN GROUP (ORDER BY)

### Examples
```python
# STRING_AGG
F.string_agg(col("name"), ", ", order_by=[col("name").asc()])
# Renders: STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY name ASC)

# FORMAT
col("date").dt.format("yyyy-MM-dd")
```

---

## 📋 Phase 6: OUTPUT Clause

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### OUTPUT Clause
- [ ] OUTPUT in INSERT/UPDATE/DELETE
- [ ] OUTPUT INSERTED.*, DELETED.*
- [ ] OUTPUT INTO table variable

### Examples
```python
# INSERT with OUTPUT
insert_into("users").values(name="Alice").output(col("INSERTED.id"))
# Renders: INSERT INTO users (name) OUTPUT INSERTED.id VALUES ('Alice')
```

---

## 📋 Phase 7: FOR XML / FOR JSON

**Status:** ❌ Not Started
**Priority:** LOW

### XML Output
- [ ] FOR XML RAW
- [ ] FOR XML AUTO
- [ ] FOR XML PATH
- [ ] FOR XML EXPLICIT

### JSON Output
- [ ] FOR JSON AUTO
- [ ] FOR JSON PATH

### Examples
```python
# FOR JSON
source("users").select(col("name"), col("email")).for_json("AUTO")
# Renders: SELECT name, email FROM users FOR JSON AUTO
```

---

## 📋 Phase 8: MERGE Statement

**Status:** ❌ Not Started
**Priority:** LOW

### MERGE Statement
- [ ] MERGE for upsert operations
- [ ] WHEN MATCHED / WHEN NOT MATCHED
- [ ] OUTPUT clause with MERGE

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Identifier quoting: Square brackets `[table]` instead of double quotes
- ⚠️ Parameter style: Named `@param` or positional
- ⚠️ TOP clause instead of LIMIT (though OFFSET/FETCH available in 2012+)
- ⚠️ CROSS APPLY / OUTER APPLY instead of LATERAL
- ⚠️ Some function names differ

### Feature Differences
- ➕ TOP clause with PERCENT and WITH TIES
- ➕ CROSS APPLY / OUTER APPLY
- ➕ OUTPUT clause for INSERT/UPDATE/DELETE
- ➕ FOR XML / FOR JSON
- ➕ MERGE statement
- ➕ TRY_CAST, TRY_CONVERT for safe casting
- ➖ No ILIKE operator (use LOWER + LIKE or COLLATE)
- ➖ No array types or operators
- ➖ Different full-text search syntax

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure
- Adapt to T-SQL syntax differences

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering (brackets, parameters)
- **Phase 2:** TOP clause 🌟 HIGH PRIORITY
- **Phase 3-8:** Additional SQL Server features as needed

---

## Priority Matrix

### HIGH Priority
1. ⭐ TOP clause (primary row limiting)

### MEDIUM Priority
2. OFFSET/FETCH (modern pagination)
3. CROSS APPLY / OUTER APPLY
4. SQL Server-specific functions (STRING_AGG, DATEADD, etc.)
5. OUTPUT clause

### LOW Priority
6. FOR XML / FOR JSON
7. MERGE statement

**Total Progress:** 0% complete (waiting on postgres infrastructure)
