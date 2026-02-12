# PostgreSQL Parity Roadmap

Feature parity tracking for `vw/postgres/` implementation.

**Status:** 🚧 In Progress
**Current Phase:** Phase 5a - Advanced Query Features (Subqueries, VALUES, CASE)
**Recently Completed:** Phase 5b (Set Operations), Phase 5c (CTEs)

---

## ✅ Completed Features

### Query Building - Basic
- ✅ SELECT with columns via `.select(*columns)`
- ✅ FROM clause (Source rendering)
- ✅ WHERE clause via `.where(*conditions)` (accumulates with AND)
- ✅ GROUP BY via `.group_by(*columns)`
- ✅ HAVING via `.having(*conditions)` (accumulates with AND)
- ✅ ORDER BY via `.order_by(*columns)`
- ✅ LIMIT via `.limit(count)`
- ✅ OFFSET via `.offset(count)`
- ✅ FETCH FIRST/NEXT via `.fetch(count, with_ties=False)` (SQL:2008 standard)
- ✅ DISTINCT via `.distinct()`

### Query Modifiers
- ✅ Generic modifiers via `.modifiers(*exprs)` (table and statement level)
- ✅ Row-level locking: FOR UPDATE, FOR SHARE, FOR NO KEY UPDATE, FOR KEY SHARE
- ✅ Lock options: SKIP LOCKED, NOWAIT
- ✅ Table sampling: TABLESAMPLE SYSTEM, TABLESAMPLE BERNOULLI
- ✅ Partition selection (PostgreSQL)

### Column References
- ✅ Unqualified columns via `col("name")`
- ✅ Qualified columns via `rowset.col("name")` (uses alias if set)
- ✅ Star expressions via `rowset.star` property
- ✅ Table aliasing via `source.alias("name")`
- ✅ Subquery aliasing via `statement.alias("name")`

### Core Infrastructure
- ✅ Source dataclass (table/view reference with modifiers support)
- ✅ Statement dataclass (SELECT query with offset, fetch, qualify)
- ✅ Column dataclass (column reference)
- ✅ Limit dataclass (LIMIT only, offset moved to Statement)
- ✅ FetchClause dataclass (FETCH FIRST n ROWS [ONLY | WITH TIES])
- ✅ Distinct dataclass (DISTINCT flag)
- ✅ Immutable dataclass pattern (frozen=True, replace())
- ✅ Factory pattern for type safety
- ✅ Source → Statement transformation
- ✅ Protocol-based abstractions (Stateful protocol)
- ✅ Standalone render() function
- ✅ Proper SQL clause ordering

### Testing
- ✅ 84 unit tests in test_base.py (describe_rowset hierarchy)
- ✅ 16 integration tests in test_query_building.py
- ✅ 37 integration tests in test_operators.py
- ✅ 16 integration tests in test_aggregate_functions.py
- ✅ 27 integration tests in test_window_functions.py
- ✅ 13 integration tests in test_window_frames.py
- ✅ 13 integration tests in test_filter_clause.py
- ✅ 10 method chaining tests
- ✅ 12 existing tests (source, column, render)
- ✅ Total: 218 tests, all passing

---

## ✅ Phase 2: Operators & Expressions

### Comparison Operators
- ✅ Equality via `col("x") == other`
- ✅ Inequality via `col("x") != other`
- ✅ Less than via `col("x") < other`
- ✅ Less than or equal via `col("x") <= other`
- ✅ Greater than via `col("x") > other`
- ✅ Greater than or equal via `col("x") >= other`

### Pattern Matching & Membership
- ✅ LIKE via `col("x").like(pattern)`
- ✅ NOT LIKE via `col("x").not_like(pattern)`
- ✅ ILIKE via `col("x").ilike(pattern)` (PostgreSQL case-insensitive)
- ✅ NOT ILIKE via `col("x").not_ilike(pattern)`
- ✅ IN via `col("x").is_in(*values)`
- ✅ NOT IN via `col("x").is_not_in(*values)`
- ✅ BETWEEN via `col("x").between(low, high)`
- ✅ NOT BETWEEN via `col("x").not_between(low, high)`

### NULL Handling
- ✅ IS NULL via `col("x").is_null()`
- ✅ IS NOT NULL via `col("x").is_not_null()`

### Logical Operators
- ✅ AND via `&` operator
- ✅ OR via `|` operator
- ✅ NOT via `~` operator

### Mathematical Operators
- ✅ Addition via `+` operator
- ✅ Subtraction via `-` operator
- ✅ Multiplication via `*` operator
- ✅ Division via `/` operator
- ✅ Modulo via `%` operator

### Expression Aliasing
- ✅ Expression aliasing via `expr.alias("name")`

### Data Structures
- ✅ Expr base class for all expression nodes
- ✅ Comparison operator states (Equals, NotEquals, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual)
- ✅ Arithmetic operator states (Add, Subtract, Multiply, Divide, Modulo)
- ✅ Logical operator states (And, Or, Not)
- ✅ Pattern matching states (Like, NotLike, IsIn, IsNotIn, Between, NotBetween)
- ✅ NULL check states (IsNull, IsNotNull)
- ✅ Expression modifier states (Alias, Cast, Asc, Desc)
- ✅ Parameter state for query parameters

---

## ✅ Phase 3: Aggregate & Window Functions

### Aggregate Functions (ANSI SQL Standard)
- ✅ COUNT(*) via `F.count()`
- ✅ COUNT(column) via `F.count(col("x"))`
- ✅ COUNT(DISTINCT column) via `F.count(col("x"), distinct=True)`
- ✅ SUM via `F.sum(col("x"))`
- ✅ AVG via `F.avg(col("x"))`
- ✅ MIN via `F.min(col("x"))`
- ✅ MAX via `F.max(col("x"))`

### Window Functions
- ✅ ROW_NUMBER() via `F.row_number()`
- ✅ RANK() via `F.rank()`
- ✅ DENSE_RANK() via `F.dense_rank()`
- ✅ NTILE(n) via `F.ntile(n)`
- ✅ LAG() via `F.lag(col("x"))`
- ✅ LEAD() via `F.lead(col("x"))`
- ✅ FIRST_VALUE() via `F.first_value(col("x"))`
- ✅ LAST_VALUE() via `F.last_value(col("x"))`
- ✅ Window OVER clause via `.over()`
- ✅ PARTITION BY via `.over(partition_by=[...])`
- ✅ Window ORDER BY via `.over(order_by=[...])`

### Window Frame Clauses
- ✅ ROWS BETWEEN via `.rows_between(start, end)`
- ✅ RANGE BETWEEN via `.range_between(start, end)`
- ✅ Frame boundaries (UNBOUNDED PRECEDING, CURRENT ROW, n PRECEDING, n FOLLOWING)
- ✅ EXCLUDE clause (NO OTHERS, CURRENT ROW, GROUP, TIES)

### Filter Clause
- ✅ FILTER (WHERE ...) for aggregates via `.filter(condition)`
- ✅ FILTER (WHERE ...) for window functions via `.filter(condition)`

### Data Structures
- ✅ Function dataclass (name, args, distinct, filter, order_by) - extended with DISTINCT and ORDER BY support
- ✅ Literal dataclass - literal values rendered as auto-generated parameters
- ✅ WindowFunction dataclass (function, partition_by, order_by, frame)
- ✅ FrameClause dataclass (mode, start, end, exclude)
- ✅ FrameBoundary dataclasses (UnboundedPreceding, UnboundedFollowing, CurrentRow, Preceding, Following)

---

## ✅ Phase 4: Joins

### Join Types
- ✅ INNER JOIN via `rowset.join.inner(right, on=[], using=[])`
- ✅ LEFT JOIN via `rowset.join.left(right, on=[], using=[])`
- ✅ RIGHT JOIN via `rowset.join.right(right, on=[], using=[])`
- ✅ FULL OUTER JOIN via `rowset.join.full_outer(right, on=[], using=[])`
- ✅ CROSS JOIN via `rowset.join.cross(right)`
- ❌ SEMI JOIN - Not standard PostgreSQL (excluded)
- ❌ ANTI JOIN - Not standard PostgreSQL (excluded)

### Join Conditions
- ✅ ON clause with multiple conditions (AND-combined)
- ✅ USING clause via `using=[col("name")]`
- ✅ Both ON and USING allowed (no validation, PostgreSQL handles errors)

### PostgreSQL-Specific Joins
- ✅ **LATERAL joins via `lateral=True` parameter**
- [ ] NATURAL JOIN (low priority)

### Join Chaining
- ✅ Multiple joins in sequence
- ✅ Mixed join types (INNER then LEFT, etc.)

### Data Structures
- ✅ Join dataclass (jtype, right, on, using)
- ✅ JoinAccessor class for .join property
- ✅ JoinType enum (INNER, LEFT, RIGHT, FULL, CROSS)

### Testing
- ✅ 15 unit tests in test_joins.py
- ✅ 18 integration tests in integration/test_joins.py

---

## ✅ Phase 5b: Set Operations

### Set Operations
- ✅ UNION via `statement1 | statement2` (remove duplicates)
- ✅ UNION ALL via `statement1 + statement2` (keep duplicates)
- ✅ INTERSECT via `statement1 & statement2`
- ✅ EXCEPT via `statement1 - statement2`
- ✅ Nested set operations with proper precedence
- ✅ Set operations can be aliased via `.alias("name")`
- ✅ Set operations can be used as subqueries
- ✅ Set operations preserve parameters from both sides
- ✅ Set operations can have ORDER BY/LIMIT applied

### Data Structures
- ✅ SetOperation dataclass (extends Source with alias support)
- ✅ SetOperation supports Reference, Statement, and nested SetOperation on both sides

### Integration Tests
- ✅ 12 integration tests in test_set_operations.py (basic, nested, with clauses)

---

## ✅ Phase 5c: CTEs (Common Table Expressions)

### CTEs
- ✅ Basic CTE via `cte(name, query)`
- ✅ Multiple CTEs via chaining `.select()` calls on CTE references
- ✅ Recursive CTEs via `cte(name, query, recursive=True)`
- ✅ CTEs with set operations as source
- ✅ CTE references in FROM, JOIN, and subqueries
- ✅ CTE column qualification via `.col("name")` and `.star`
- ✅ CTE aliasing via `.alias("name")`

### Data Structures
- ✅ CTE dataclass (extends Statement)
- ✅ EXISTS expression support

### Integration Tests
- ✅ 14 integration tests in test_ctes.py (basic, nested, recursive, complex scenarios)

---

## 📋 Phase 5a: Advanced Query Features (In Progress)

### Subqueries
- ✅ Subqueries in FROM (Statement as source)
- ✅ Subqueries in WHERE with EXISTS via `exists(subquery)`
- ✅ Subqueries in WHERE with IN via `col("x").is_in(subquery)`
- ✅ Scalar subqueries in SELECT via `select(subquery.alias("x"))`
- ✅ Scalar subqueries in comparisons via `col("x") > subquery`
- ✅ Correlated subqueries

### VALUES Clause
- ✅ VALUES as row source via `values(alias, *rows)`
- ✅ VALUES with aliasing (alias required at construction time)
- ✅ VALUES with column list (derived from row dict keys)

### Conditional Expressions
- ✅ CASE WHEN via `when(condition).then(value).otherwise(default)`
- ✅ Multiple WHEN clauses
- ✅ Nested CASE expressions

### Data Structures Needed
- ✅ Values dataclass (row value constructor)
- ✅ Case dataclass (CASE expression)
- ✅ WhenThen dataclass (WHEN/THEN pair in CASE)

---

## ✅ Phase 6: Parameters & Rendering

### Parameters
- ✅ Parameter support via `param(name, value)`
- ✅ Supported types: str, int, float, bool, None
- ✅ Parameter reuse across query
- ✅ Type validation in param()
- ✅ Parameter styles: COLON (`:name`), DOLLAR (`$name`), AT (`@name`), PYFORMAT (`%(name)s`)

### Rendering System
- ✅ Basic render() function returns SQL dataclass
- ✅ RenderContext for parameter collection
- ✅ SQL dataclass with query and params dict
- ✅ Configurable parameter style via RenderConfig
- ✅ Dialect-specific rendering via isinstance() checks in vw/postgres/render.py

### Data Structures
- ✅ Parameter dataclass (in vw/core/states.py)
- ✅ RenderContext class (in vw/core/render.py)
- ✅ SQL class (in vw/core/render.py)
- ✅ RenderConfig class (in vw/core/render.py)
- ✅ ParamStyle enum (in vw/core/render.py)

---

## 📋 Phase 7: Scalar Functions

### String Functions
- [x] UPPER via `col("x").text.upper()`
- [x] LOWER via `col("x").text.lower()`
- [x] TRIM via `col("x").text.trim()`
- [x] LTRIM via `col("x").text.ltrim()`
- [x] RTRIM via `col("x").text.rtrim()`
- [x] LENGTH via `col("x").text.length()`
- [x] SUBSTRING via `col("x").text.substring(start, length)`
- [x] REPLACE via `col("x").text.replace(old, new)`
- [x] CONCAT via `col("x").text.concat(*others)`
- [x] String concatenation operator `||` via `expr.op("||", other)`

### Date/Time Functions
- [x] CURRENT_TIMESTAMP via `F.current_timestamp()`
- [x] CURRENT_DATE via `F.current_date()`
- [x] CURRENT_TIME via `F.current_time()`
- [x] NOW() via `F.now()` (PostgreSQL-specific)
- [x] EXTRACT via `col("x").dt.extract(field)` (e.g. "year", "month", "day", "hour", "minute", "second")
- [x] EXTRACT shortcuts: `col("x").dt.quarter()`, `.dt.week()`, `.dt.weekday()` (DOW)
- [x] Interval arithmetic via `+` and `-` with intervals
- [x] INTERVAL creation via `interval(value, unit)` (PostgreSQL-specific)
- [x] DATE_TRUNC via `col("x").dt.date_trunc(unit)` (PostgreSQL-specific)

### Null Handling Functions
- [x] COALESCE via `F.coalesce(*values)`
- [x] NULLIF via `F.nullif(value1, value2)`
- [x] GREATEST via `F.greatest(*values)`
- [x] LEAST via `F.least(*values)`

### Type Casting
- [x] CAST via `col("x").cast(dtype)`
- [x] Type constructors via `vw.core.types` and `vw.postgres.types` functions
- [ ] Dialect-specific type name aliasing (future work)

### Data Structures Needed
- [x] TextAccessor class for .text property
- [x] DateTimeAccessor class for .dt property
- [x] Interval dataclass (PostgreSQL-specific)
- [x] Cast dataclass
- [x] Type system (`vw.core.types` and `vw.postgres.types` modules)

---

## 📋 Phase 8: PostgreSQL-Specific Features

### Strategy: Raw SQL API + High-Value Conveniences

**Philosophy**: Use `raw.expr()` and `raw.rowset()` for most PostgreSQL-specific features instead of wrapping everything. Only add convenience wrappers for extremely common patterns that improve ergonomics significantly.

**Already available via raw SQL API:**
- ✅ `raw.expr(template, **kwargs)` - raw SQL expressions with safe parameter substitution
- ✅ `raw.rowset(template, **kwargs)` - raw SQL sources/table expressions
- ✅ All operators via `expr.op("->", other)` - works for any infix operator
- ✅ Documentation: See `/Users/ryan/github/vw/vw/core/raw.py` for examples

### PostgreSQL Extensions
- [x] DISTINCT ON via `.distinct(col("x"), ...)` (PostgreSQL-specific override)
- [x] **LATERAL joins** (completed - available via `lateral=True` parameter)

### Raw SQL API Enhancements
- ✅ `raw.func(name, *args)` - convenience for function calls
- [ ] PostgreSQL Cookbook documentation showing common raw.expr() patterns

### PostgreSQL Data Types & Functions

**All available via raw.expr()** - no wrappers needed:
- ✅ JSONB literals: `raw.expr("'{...}'::jsonb")`
- ✅ JSON functions: `raw.expr("jsonb_build_object({k}, {v})", ...)`
- [x] JSON operators: via `expr.op("->", other)` (already completed)
- ✅ ARRAY literals: `raw.expr("ARRAY[{a}, {b}]", ...)`
- ✅ Array functions: `raw.expr("unnest({arr})", ...)`
- [x] Array operators: via `expr.op("@>", other)` (already completed)
- ✅ UUID functions: `raw.expr("gen_random_uuid()")`
- ✅ Text search: `raw.expr("to_tsvector({text})", ...)`
- [x] Text search operator: via `expr.op("@@", other)` (already completed)
- [x] Regex operators: via `expr.op("~", other)` (already completed)
- ✅ Geometric types: `raw.expr("ST_Distance({a}, {b})", ...)`
- ✅ Any PostgreSQL function or type

**High-value convenience wrappers (Tier 1 & 2):**
- ✅ `lit(value)` - literal values rendered as auto-generated parameters
- ✅ `raw.func(name, *args)` - ergonomic shorthand for simple function calls
- ✅ `F.gen_random_uuid()` - UUID generation (ubiquitous, zero-config)
- ✅ `F.array_agg(expr, order_by=None, distinct=False)` - array aggregation with ORDER BY support
- ✅ `F.string_agg(expr, separator, order_by=None)` - string aggregation with ORDER BY support
- ✅ `F.json_build_object(*args)` - JSON object construction (variadic args much cleaner)
- ✅ `F.json_agg(expr, order_by=None)` - JSON array aggregation with ORDER BY support
- ✅ `F.unnest(array)` - expand arrays to rows (common in SELECT clause)

**Future convenience wrappers to consider:**
- [ ] `F.to_tsvector(config, text)` - full-text search foundation
- [ ] `F.to_tsquery(config, query)` - full-text search foundation

**Not needed** (use raw.expr() instead):
- ❌ Dozens of JSON functions (json_extract_path, jsonb_set, jsonb_insert, etc.)
- ❌ Dozens of array functions (array_length, array_position, array_append, etc.)
- ❌ Geometric functions (better served by PostGIS extension patterns)
- ❌ HSTORE functions (legacy, superseded by JSONB)
- ❌ Network type functions (niche use case)
- ❌ Hundreds of other PostgreSQL-specific functions

### PostgreSQL Advanced Features
- [x] FOR UPDATE / FOR SHARE locking (via `.modifiers()`)
- [x] Table sampling (TABLESAMPLE via `.modifiers()`)
- [x] GROUPING SETS
- [x] CUBE
- [x] ROLLUP
- [x] FILTER clause (completed in Phase 3)
- [x] FETCH FIRST/NEXT (SQL:2008 standard)

---

## Testing Requirements

Each phase should include:
- [ ] Unit tests for new methods
- [ ] Integration tests with sql() utility
- [ ] Test coverage for edge cases
- [ ] Test coverage for error conditions
- [ ] Documentation in docstrings

---

## Current Status Summary

**Completed:**
- Phase 1: Core Query Building ✅
- Phase 2: Operators & Expressions ✅
- Phase 3: Aggregate & Window Functions ✅
- Phase 4: Joins ✅
- Phase 5b: Set Operations ✅
- Phase 5c: CTEs (Common Table Expressions) ✅
- Phase 6: Parameters & Rendering ✅

**In Progress:**
- Phase 5a: Advanced Query Features (Subqueries, VALUES, CASE)

**Remaining:**
- Phase 7: Scalar Functions
- Phase 8: PostgreSQL-Specific Features

**Total Progress:** ~70% complete (7/10 phases, with 5a partially complete)
