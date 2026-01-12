# Features

## Query Building
- [x] Basic SELECT statements
- [x] SELECT * FROM table
- [x] SELECT * {EXCLUDE,REPLACE,RENAME} (...) FROM table (`vw.star`, `Source.star`)
- [x] SELECT col1, col2 FROM table
- [x] Method chaining (Source → join → select → render)
- [x] Qualified column references via `Source.col()`
- [x] WHERE clause support
- [x] GROUP BY / HAVING
- [x] ORDER BY (with `.asc()` and `.desc()`)
- [x] LIMIT / OFFSET (with dialect-aware rendering for SQL Server)
- [x] DISTINCT (via `.distinct()` method)
- [x] DISTINCT ON (via `.distinct(on=[...])` - PostgreSQL only)

## Joins
- [x] INNER JOIN support with accessor pattern (`Source.join.inner()`)
- [x] LEFT JOIN support with accessor pattern (`Source.join.left()`)
- [x] RIGHT JOIN support with accessor pattern (`Source.join.right()`)
- [x] FULL OUTER JOIN support with accessor pattern (`Source.join.full_outer()`)
- [x] CROSS JOIN support with accessor pattern (`Source.join.cross()`)
- [x] SEMI JOIN support with accessor pattern (`Source.join.semi()`)
- [x] ANTI JOIN support with accessor pattern (`Source.join.anti()`)
- [x] Multiple ON conditions in joins (combined with AND)
- [x] Chaining multiple joins
- [x] Mixed join types in chain (e.g., inner then left)

## Parameters
- [x] Parameterized values via `param(name, value)`
- [x] Supported types: str, int, float, bool
- [x] Parameter reuse in multiple places
- [x] Type validation in param() function
- [x] Multiple parameter styles (colon, dollar, at)

## Operators
- [x] Equality comparison (`==`)
- [x] Inequality comparison (`!=`)
- [x] Less than comparison (`<`)
- [x] Less than or equal comparison (`<=`)
- [x] Greater than comparison (`>`)
- [x] Greater than or equal comparison (`>=`)
- [x] LIKE / NOT LIKE pattern matching (via `.like()`, `.not_like()`)
- [x] IN comparison via `.is_in()` / `.is_not_in()`
- [x] BETWEEN / NOT BETWEEN range checks (via `.between()`, `.not_between()`)
- [x] Mathematical operators: `+`, `-`, `*`, `/`, `%`
- [x] Logical operators: AND, OR
- [x] Logical operators: NOT

## Functions
- [x] Aggregate functions: COUNT, SUM, AVG, MIN, MAX (via `F.count`, `F.sum`, `F.avg`, `F.min`, `F.max`)
- [x] Window functions (via `F` namespace with `.over()`)
- [x] Window function frame clauses (ROWS/RANGE BETWEEN via `.rows_between()`, `.range_between()`)
- [x] Window frame boundaries (via `vw.frame` module: UNBOUNDED_PRECEDING, CURRENT_ROW, etc.)
- [x] String operations (via `.text` accessor: upper, lower, trim, ltrim, rtrim, length, substring, replace, concat)
- [x] Date/time operations (via `.dt` accessor: year, quarter, month, week, day, hour, minute, second, weekday, truncate, date, time; standalone: current_timestamp, current_date, current_time, now)
- [x] Interval arithmetic via `.dt.date_add()`, `.dt.date_sub()`
- [x] Standalone interval creation via `interval()` function
- [x] Type casting (`.cast()` method with dialect-aware rendering)
- [x] SQL type constructors (e.g., `vw.dtypes.varchar(255)`, `vw.dtypes.decimal(10, 2)`) to prevent typos
- [x] NULL handling: `.is_null()` and `.is_not_null()` methods
- [x] COALESCE function (via `F.coalesce`)
- [x] NULLIF function (via `F.nullif`)
- [x] GREATEST / LEAST functions (via `F.greatest`, `F.least`)
- [x] CASE expressions (via `vw.when().then().otherwise()`)

## Advanced Query Features
- [x] Subqueries in FROM/JOIN (via Statement as RowSet)
- [x] Table/subquery aliasing via `.alias()`
- [x] Expression aliasing via `.alias()` (column AS name)
- [x] Subqueries in WHERE: IN via `.is_in()` / `.is_not_in()`
- [x] Subqueries in WHERE: EXISTS via `vw.exists()`
- [x] Scalar subqueries in comparisons (e.g., `col("price") > subquery`)
- [x] CTEs (Common Table Expressions / WITH clause)
- [x] UNION / UNION ALL / INTERSECT / EXCEPT via operators (`|`, `+`, `&`, `-`)

## Rendering
- [x] Expression Protocol with `__vw_render__(context)`
- [x] RenderContext for parameter collection
- [x] RenderResult with SQL and params dict
- [x] Configurable parameter styles
- [x] Parameter style override via `RenderConfig.param_style`

## Escape Hatch
- [x] Raw SQL strings in columns for unsupported features
- [x] Star extensions (REPLACE, EXCLUDE)
- [x] Complex expressions (CAST, ROUND, etc.)

## Developer Experience
- [ ] Fluent API for star extensions: `col("*").replace(foo="bar").exclude("baz")`
- [ ] Better error messages
- [ ] SQL formatting/pretty-printing

## Error Handling
- [x] Custom exception hierarchy with base `VWError`
- [x] `CTENameCollisionError` for duplicate CTE names
- [x] `UnsupportedParamStyleError` for invalid parameter styles
- [x] `UnsupportedDialectError` for dialect-specific limitations


