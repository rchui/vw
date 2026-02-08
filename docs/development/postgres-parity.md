# PostgreSQL Parity Roadmap

Feature parity tracking for `vw/postgres/` implementation vs `vw/reference/`.

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
- ✅ OFFSET via `.limit(count, offset=n)`
- ✅ DISTINCT via `.distinct()`

### Column References
- ✅ Unqualified columns via `col("name")`
- ✅ Qualified columns via `rowset.col("name")` (uses alias if set)
- ✅ Star expressions via `rowset.star` property
- ✅ Table aliasing via `source.alias("name")`
- ✅ Subquery aliasing via `statement.alias("name")`

### Core Infrastructure
- ✅ Source dataclass (table/view reference)
- ✅ Statement dataclass (SELECT query)
- ✅ Column dataclass (column reference)
- ✅ Limit dataclass (LIMIT/OFFSET)
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
- ⏭️ ILIKE via `col("x").ilike(pattern)` (PostgreSQL case-insensitive) - deferred
- ⏭️ NOT ILIKE via `col("x").not_ilike(pattern)` - deferred
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
- ✅ Function dataclass (name, args, filter)
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
- [ ] LATERAL joins via `lateral=True` parameter (deferred to Phase 5)
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
- [ ] UPPER via `col("x").text.upper()`
- [ ] LOWER via `col("x").text.lower()`
- [ ] TRIM via `col("x").text.trim()`
- [ ] LTRIM via `col("x").text.ltrim()`
- [ ] RTRIM via `col("x").text.rtrim()`
- [ ] LENGTH via `col("x").text.length()`
- [ ] SUBSTRING via `col("x").text.substring(start, length)`
- [ ] REPLACE via `col("x").text.replace(old, new)`
- [ ] CONCAT via `col("x").text.concat(other)` or `+` operator
- [ ] String concatenation operator `||`

### Date/Time Functions
- [ ] CURRENT_TIMESTAMP via `F.current_timestamp()`
- [ ] CURRENT_DATE via `F.current_date()`
- [ ] CURRENT_TIME via `F.current_time()`
- [ ] NOW() via `F.now()`
- [ ] EXTRACT YEAR via `col("x").dt.year()`
- [ ] EXTRACT MONTH via `col("x").dt.month()`
- [ ] EXTRACT DAY via `col("x").dt.day()`
- [ ] EXTRACT HOUR via `col("x").dt.hour()`
- [ ] EXTRACT MINUTE via `col("x").dt.minute()`
- [ ] EXTRACT SECOND via `col("x").dt.second()`
- [ ] DATE_TRUNC via `col("x").dt.truncate(unit)`
- [ ] Interval arithmetic via `+` and `-` with intervals
- [ ] INTERVAL creation via `interval(value, unit)`

### Null Handling Functions
- [ ] COALESCE via `F.coalesce(*values)`
- [ ] NULLIF via `F.nullif(value1, value2)`
- [ ] GREATEST via `F.greatest(*values)`
- [ ] LEAST via `F.least(*values)`

### Type Casting
- [ ] CAST via `col("x").cast(dtype)`
- [ ] Type constructors (VARCHAR, INTEGER, TIMESTAMP, etc.)
- [ ] Dialect-specific type mapping

### Data Structures Needed
- [ ] TextAccessor class for .text property
- [ ] DateTimeAccessor class for .dt property
- [ ] Interval dataclass
- [ ] Cast dataclass
- [ ] Type system (dtype module)

---

## 📋 Phase 8: DML Statements

### INSERT
- [ ] INSERT with VALUES via `source("table").insert(values(...))`
- [ ] INSERT from SELECT via `source("table").insert(query)`
- [ ] INSERT with column list via `source("table").insert(..., columns=[...])`
- [ ] RETURNING clause via `.returning(*columns)`
- [ ] ON CONFLICT DO NOTHING via `.on_conflict().do_nothing()`
- [ ] ON CONFLICT DO UPDATE (upsert) via `.on_conflict().do_update(...)`

### UPDATE
- [ ] Basic UPDATE via `source("table").update()`
- [ ] SET clause via `.set(col("x"), value)` or `.set({col: value})`
- [ ] WHERE clause via `.where(*conditions)`
- [ ] FROM clause (PostgreSQL) via `.from_(...)`
- [ ] RETURNING clause via `.returning(*columns)`

### DELETE
- [ ] Basic DELETE via `source("table").delete()`
- [ ] WHERE clause via `.where(*conditions)`
- [ ] USING clause (PostgreSQL) via `.using(*rowsets)`
- [ ] RETURNING clause via `.returning(*columns)`

### Data Structures Needed
- [ ] Insert dataclass
- [ ] Update dataclass
- [ ] Delete dataclass
- [ ] Returning dataclass
- [ ] OnConflict dataclass

---

## 📋 Phase 9: DDL Statements

### Table Operations
- [ ] CREATE TABLE via `source("table").table.create()`
- [ ] Column definitions via `.add_column(name, dtype, ...)`
- [ ] Primary key via `.primary_key([...])`
- [ ] Foreign key via `.foreign_key(...)`
- [ ] CREATE IF NOT EXISTS via `.if_not_exists()`
- [ ] CREATE OR REPLACE via `.or_replace()`
- [ ] CREATE TEMPORARY via `.temporary()`
- [ ] CREATE TABLE AS SELECT via `.as_select(query)`
- [ ] DROP TABLE via `source("table").table.drop()`
- [ ] DROP IF EXISTS via `.if_exists()`
- [ ] DROP CASCADE via `.cascade()`

### View Operations
- [ ] CREATE VIEW via `source("view").view.create(query)`
- [ ] CREATE OR REPLACE VIEW via `.or_replace()`
- [ ] CREATE MATERIALIZED VIEW (PostgreSQL)
- [ ] DROP VIEW via `source("view").view.drop()`
- [ ] DROP IF EXISTS via `.if_exists()`
- [ ] DROP CASCADE via `.cascade()`

### Index Operations
- [ ] CREATE INDEX via `source("table").index.create(name, columns)`
- [ ] CREATE UNIQUE INDEX via `.unique()`
- [ ] DROP INDEX via `source("table").index.drop(name)`

### Data Structures Needed
- [ ] CreateTable dataclass
- [ ] ColumnDef dataclass
- [ ] Constraint dataclass
- [ ] DropTable dataclass
- [ ] CreateView dataclass
- [ ] DropView dataclass
- [ ] TableAccessor class
- [ ] ViewAccessor class
- [ ] IndexAccessor class

---

## 📋 Phase 10: PostgreSQL-Specific Features

### PostgreSQL Extensions
- [ ] DISTINCT ON via `.distinct(on=[...])`
- [ ] LATERAL joins (already in Phase 4)
- [ ] RETURNING clause (already in Phase 8)
- [ ] INSERT ... ON CONFLICT (already in Phase 8)
- [ ] UPDATE ... FROM (already in Phase 8)
- [ ] DELETE ... USING (already in Phase 8)

### PostgreSQL Data Types
- [ ] JSONB support
- [ ] JSON operators (`->`, `->>`, `@>`, etc.)
- [ ] ARRAY types and literals
- [ ] Array operators and functions
- [ ] UUID type
- [ ] HSTORE type
- [ ] Geometric types (POINT, LINE, etc.)

### PostgreSQL Functions
- [ ] STRING_AGG via `F.string_agg(col("x"), separator)` (PostgreSQL syntax, not ANSI LISTAGG)
- [ ] ARRAY_AGG via `F.array_agg(col("x"))` (ANSI SQL:2003 but deferred from Phase 3)
- [ ] JSON functions (json_extract_path, etc.)
- [ ] Array functions (array_length, unnest, etc.)
- [ ] Regex operators (`~`, `~*`, `!~`, `!~*`)
- [ ] Full-text search (tsvector, tsquery, @@)

### PostgreSQL Advanced Features
- [ ] FOR UPDATE / FOR SHARE locking
- [ ] GROUPING SETS
- [ ] CUBE
- [ ] ROLLUP
- [ ] FILTER clause (already in Phase 3)

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
- Phase 8: DML Statements
- Phase 9: DDL Statements
- Phase 10: PostgreSQL-Specific Features

**Total Progress:** ~60% complete (7/12 phases, with 5a partially complete)
