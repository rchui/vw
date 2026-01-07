# Features

## Query Building
- [x] Basic SELECT statements
- [x] SELECT * FROM table
- [x] SELECT col1, col2 FROM table
- [x] Method chaining (Source → join → select → render)
- [x] Qualified column references via `Source.col()`
- [x] WHERE clause support
- [x] GROUP BY / HAVING
- [x] ORDER BY (with `.asc()` and `.desc()`)
- [x] LIMIT / OFFSET (with dialect-aware rendering for SQL Server)
- [ ] DISTINCT

## Joins
- [x] INNER JOIN support with accessor pattern (`Source.join.inner()`)
- [x] Multiple ON conditions in joins (combined with AND)
- [x] Chaining multiple joins
- [x] Cross joins (INNER JOIN without ON condition)
- [ ] LEFT JOIN / LEFT OUTER JOIN
- [ ] RIGHT JOIN / RIGHT OUTER JOIN
- [ ] FULL OUTER JOIN
- [ ] ANTI JOIN
- [ ] SEMI JOIN
- [ ] CROSS JOIN (explicit)

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
- [ ] LIKE comparison
- [ ] IN comparison
- [x] Logical operators: AND, OR
- [x] Logical operators: NOT

## Functions
- [ ] Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- [ ] Window functions
- [ ] String operations
- [ ] Date/time operations
- [x] Type casting (`.cast()` method with dialect-aware rendering)
- [ ] SQL type constructors (e.g., `vw.types.varchar(255)`, `vw.types.decimal(10, 2)`) to prevent typos
- [ ] NULL handling (IS NULL, IS NOT NULL, COALESCE)
- [ ] CASE expressions

## Advanced Query Features
- [x] Subqueries in FROM/JOIN (via Statement as RowSet)
- [x] Table/subquery aliasing via `.alias()`
- [x] Expression aliasing via `.alias()` (column AS name)
- [ ] Subqueries in WHERE (IN, EXISTS)
- [x] CTEs (Common Table Expressions / WITH clause)
- [ ] UNION / UNION ALL / INTERSECT / EXCEPT

## Rendering
- [x] Expression Protocol with `__vw_render__(context)`
- [x] RenderContext for parameter collection
- [x] RenderResult with SQL and params dict
- [x] Configurable parameter styles

## Escape Hatch
- [x] Raw SQL strings in columns for unsupported features
- [x] Star extensions (REPLACE, EXCLUDE)
- [x] Complex expressions (CAST, ROUND, etc.)

## Developer Experience
- [ ] Fluent API for star extensions: `col("*").replace(foo="bar").exclude("baz")`
- [ ] Better error messages
- [ ] SQL formatting/pretty-printing

## Infrastructure
- [ ] CI/CD configuration
- [ ] Expand CLI functionality beyond basic --version flag
- [ ] Documentation site
- [ ] Performance benchmarks
- [ ] Database driver integration examples
