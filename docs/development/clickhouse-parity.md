# ClickHouse Parity Roadmap

Feature parity tracking for `vw/clickhouse/` implementation.

**Status:** ❌ Not Started
**Current Phase:** None - waiting for core infrastructure from postgres
**Prerequisites:** Most PostgreSQL phases must be completed first

---

## Strategy

ClickHouse implementation will reuse core infrastructure from `vw/core/` and `vw/postgres/`, adapting to ClickHouse's OLAP-optimized columnar database features.

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

**ClickHouse-Specific (new implementation required):**
- Array functions and operators (native arrays)
- ARRAY JOIN for unnesting arrays
- FINAL modifier for collapsing tables
- PREWHERE optimization hint
- SAMPLE clause for sampling
- ClickHouse-specific aggregate functions (uniq, quantile variants, etc.)
- Tuple types and functions
- Table engines (MergeTree, etc.)
- Materialized views
- ClickHouse-specific string and date functions
- LowCardinality type

---

## Prerequisites (Inherited from PostgreSQL)

These phases from PostgreSQL will be automatically available in ClickHouse:

### ✅ Phase 1-7: Core Features (inherited)
- ✅ Query building, operators, expressions
- ✅ Aggregate and window functions
- ✅ Joins, subqueries, CTEs
- ✅ Parameters and rendering
- ✅ Scalar functions

---

## 📋 Phase 1: Core ClickHouse Implementation

**Status:** ❌ Not Started

### Infrastructure Setup
- [ ] Create `vw/clickhouse/` directory
- [ ] Create `vw/clickhouse/base.py` with ClickHouse-specific classes
- [ ] Create `vw/clickhouse/public.py` for public API
- [ ] Create `vw/clickhouse/render.py` for ClickHouse SQL rendering
- [ ] Set up `vw/clickhouse/__init__.py` exports
- [ ] Create `tests/clickhouse/` directory structure

### ClickHouse-Specific Rendering
- [ ] Handle identifier quoting (backticks or double quotes)
- [ ] Parameter style (named `{param:Type}` or positional)
- [ ] Type name differences from PostgreSQL
- [ ] Function name variations
- [ ] Override PostgreSQL-specific features

### Testing Setup
- [ ] Set up test database fixtures (ClickHouse test instance)
- [ ] Port relevant postgres tests to clickhouse
- [ ] Create ClickHouse-specific test utilities
- [ ] Verify inherited features work correctly

---

## 📋 Phase 2: Array Operations

**Status:** ❌ Not Started
**Priority:** HIGH - Core ClickHouse feature

### Array Type and Functions
- [ ] Array type support
- [ ] Array literals: `[1, 2, 3]`
- [ ] `array()` function - construct array
- [ ] `arrayElement()` - access array element
- [ ] `arraySlice()` - slice array
- [ ] `arrayConcat()` - concatenate arrays
- [ ] `has()` - check array membership
- [ ] `length()` - array length
- [ ] `arrayMap()` - map function over array
- [ ] `arrayFilter()` - filter array
- [ ] `arrayReduce()` - reduce array with aggregate

### ARRAY JOIN
- [ ] `ARRAY JOIN` for unnesting arrays
- [ ] `LEFT ARRAY JOIN`
- [ ] Via `.array_join(array_col, alias)`

### Data Structures Needed
- [ ] ArrayType dataclass
- [ ] ArrayJoin dataclass
- [ ] Array function wrappers in F module

### Examples
```python
# Array construction
F.array(1, 2, 3, 4, 5)
# Renders: [1, 2, 3, 4, 5]

# Array element access
col("tags")[1]
# Renders: tags[1]

# ARRAY JOIN
source("events").array_join(col("tags"), "tag")
# Renders: SELECT * FROM events ARRAY JOIN tags AS tag

# Array functions
F.array_map(lambda x: x * 2, col("numbers"))
```

---

## 📋 Phase 3: ClickHouse-Specific Aggregate Functions

**Status:** ❌ Not Started
**Priority:** HIGH - OLAP-optimized aggregates

### Approximate Aggregates
- [ ] `uniq()` - approximate distinct count (HyperLogLog)
- [ ] `uniqExact()` - exact distinct count
- [ ] `uniqCombined()` - combined distinct count
- [ ] `uniqHLL12()` - HyperLogLog with 12-bit precision

### Quantile Functions
- [ ] `quantile()` - approximate quantile
- [ ] `quantileExact()` - exact quantile
- [ ] `quantileTiming()` - quantile for timing data
- [ ] `quantileTDigest()` - quantile using t-digest
- [ ] `quantiles()` - multiple quantiles at once

### Statistical Aggregates
- [ ] `varPop()` - population variance
- [ ] `varSamp()` - sample variance
- [ ] `stddevPop()` - population standard deviation
- [ ] `stddevSamp()` - sample standard deviation
- [ ] `corr()` - correlation coefficient
- [ ] `covarPop()` - population covariance
- [ ] `covarSamp()` - sample covariance

### Other Aggregates
- [ ] `groupArray()` - aggregate to array
- [ ] `groupUniqArray()` - aggregate to array of distinct values
- [ ] `groupArraySample()` - random sample aggregate
- [ ] `topK()` - top K values
- [ ] `groupBitOr()` / `groupBitAnd()` / `groupBitXor()` - bitwise aggregates

### Examples
```python
# Approximate distinct count
F.uniq(col("user_id"))
# Renders: uniq(user_id)

# Quantile
F.quantile(0.95, col("response_time"))
# Renders: quantile(0.95)(response_time)

# Top K
F.topK(10, col("product_id"))
# Renders: topK(10)(product_id)
```

---

## 📋 Phase 4: SAMPLE and PREWHERE

**Status:** ❌ Not Started
**Priority:** MEDIUM - Performance optimization

### SAMPLE Clause
- [ ] `SAMPLE factor` - sample fraction of data
- [ ] `SAMPLE n` - sample n rows
- [ ] Via `.sample(factor)` or `.sample(rows=n)`

### PREWHERE Clause
- [ ] PREWHERE for optimized filtering (ClickHouse-specific)
- [ ] Automatically use PREWHERE for simple filters
- [ ] Via `.prewhere(condition)` or automatic detection

### Data Structures Needed
- [ ] Sample dataclass
- [ ] Prewhere support in Statement

### Examples
```python
# SAMPLE
source("events").sample(0.1)
# Renders: SELECT * FROM events SAMPLE 0.1

# PREWHERE (manual)
source("events").prewhere(col("date") >= "2024-01-01")
# Renders: SELECT * FROM events PREWHERE date >= '2024-01-01'
```

---

## 📋 Phase 5: FINAL Modifier

**Status:** ❌ Not Started
**Priority:** MEDIUM - Important for MergeTree tables

### FINAL Modifier
- [ ] `SELECT * FROM table FINAL` - collapse ReplacingMergeTree
- [ ] Via `.final()` method on source
- [ ] Applies to tables with MergeTree engines

### Examples
```python
# Query with FINAL
source("users").final().select(col("name"))
# Renders: SELECT name FROM users FINAL
```

---

## 📋 Phase 6: Tuple Type and Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### Tuple Type
- [ ] Tuple type support
- [ ] Tuple literals: `(1, 'string', 3.14)`
- [ ] Tuple element access: `tuple.1`, `tuple.2`
- [ ] Named tuples

### Tuple Functions
- [ ] `tuple()` - construct tuple
- [ ] `tupleElement()` - access tuple element

### Examples
```python
# Tuple construction
F.tuple(col("a"), col("b"), col("c"))
# Renders: (a, b, c)

# Tuple element access
col("my_tuple")[1]
```

---

## 📋 Phase 7: ClickHouse-Specific Functions

**Status:** ❌ Not Started
**Priority:** MEDIUM-LOW

### String Functions
- [ ] `splitByChar()` - split string by character
- [ ] `extractAll()` - extract all matches by regex
- [ ] `replaceAll()` / `replaceOne()` / `replaceRegexpAll()` - replace operations

### Date/Time Functions
- [ ] `toDateTime()` - convert to datetime
- [ ] `toDate()` - convert to date
- [ ] `now()` - current timestamp
- [ ] `today()` - current date
- [ ] `formatDateTime()` - format datetime
- [ ] `parseDateTimeBestEffort()` - parse datetime

### Hash Functions
- [ ] `cityHash64()` - CityHash
- [ ] `xxHash64()` - xxHash
- [ ] `sipHash64()` - SipHash

---

## 📋 Phase 8: Table Engines and DDL

**Status:** ❌ Not Started
**Priority:** LOW

### Table Engines
- [ ] Specify table engine in CREATE TABLE
- [ ] MergeTree family
- [ ] ReplacingMergeTree
- [ ] SummingMergeTree
- [ ] AggregatingMergeTree
- [ ] CollapsingMergeTree

### Materialized Views
- [ ] CREATE MATERIALIZED VIEW
- [ ] Reference materialized views

---

## Key Differences from PostgreSQL

### Syntax Differences
- ⚠️ Parameter style: Named `{param:Type}` or positional
- ⚠️ Array syntax: `[1, 2, 3]` for literals, `col[index]` for access
- ⚠️ ARRAY JOIN instead of UNNEST
- ⚠️ PREWHERE clause (optimization)
- ⚠️ FINAL modifier for collapsing tables

### Feature Differences
- ➕ Native array support with rich operations
- ➕ ARRAY JOIN for unnesting
- ➕ Approximate aggregates (uniq, quantile variants)
- ➕ SAMPLE clause for sampling
- ➕ PREWHERE optimization
- ➕ FINAL modifier
- ➕ Tuple types
- ➕ MergeTree table engines
- ➕ LowCardinality type
- ➖ No ILIKE operator (use LIKE with lower())
- ➖ Different full-text search
- ➖ Limited UPDATE/DELETE (for MergeTree)
- ➖ No transactions (OLAP database)

### Performance Characteristics
- Columnar storage optimized for OLAP
- Highly parallel query execution
- Optimized for analytical workloads

---

## Implementation Strategy

### 1. Start After Core PostgreSQL Complete
- **Prerequisites:** PostgreSQL Phases 1-7 must be complete
- Inherit all core infrastructure
- Adapt to ClickHouse's OLAP-focused features

### 2. Incremental Approach
- **Phase 1:** Basic setup and rendering
- **Phase 2:** Array operations and ARRAY JOIN 🌟 HIGH PRIORITY
- **Phase 3:** ClickHouse aggregate functions 🌟 HIGH PRIORITY
- **Phase 4-8:** Additional ClickHouse features as needed

---

## Priority Matrix

### HIGH Priority (Core ClickHouse Value)
1. ⭐ Array operations and ARRAY JOIN
2. ⭐ Approximate aggregates (uniq, quantile variants)

### MEDIUM Priority (Common Use Cases)
3. SAMPLE clause
4. PREWHERE optimization
5. FINAL modifier
6. Tuple types

### LOW Priority (Nice to Have)
7. ClickHouse-specific functions
8. Table engines and DDL

**Total Progress:** 0% complete (waiting on postgres infrastructure)
