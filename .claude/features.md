# Features

## Implemented

### Query Building
- ✅ Basic SELECT statements
- ✅ SELECT * FROM table
- ✅ SELECT col1, col2 FROM table
- ✅ Method chaining (Source → join → select → render)
- ✅ Qualified column references via `Source.col()`

### Joins
- ✅ INNER JOIN support with accessor pattern (`Source.join.inner()`)
- ✅ Multiple ON conditions in joins (combined with AND)
- ✅ Chaining multiple joins
- ✅ Cross joins (INNER JOIN without ON condition)

### Parameters
- ✅ Parameterized values via `param(name, value)`
- ✅ Supported types: str, int, float, bool
- ✅ Parameter reuse in multiple places
- ✅ Type validation in param() function
- ✅ Multiple parameter styles (colon, dollar, at)

### Operators
- ✅ Equality comparison (`==`)
- ✅ Inequality comparison (`!=`)

### Rendering
- ✅ Expression Protocol with `__vw_render__(context)`
- ✅ RenderContext for parameter collection
- ✅ RenderResult with SQL and params dict
- ✅ Configurable parameter styles

### Escape Hatch
- ✅ Raw SQL strings in columns for unsupported features
- ✅ Star extensions (REPLACE, EXCLUDE)
- ✅ Complex expressions (CAST, ROUND, etc.)

## Future Considerations

### Query Features
- WHERE clause support
- Additional comparison operators: `<`, `>`, `<=`, `>=`, `LIKE`, `IN`
- Logical operators: AND, OR, NOT
- GROUP BY / HAVING
- ORDER BY / LIMIT / OFFSET
- DISTINCT
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Window functions
- UNION / UNION ALL / INTERSECT / EXCEPT

### Join Types
- LEFT JOIN / LEFT OUTER JOIN
- RIGHT JOIN / RIGHT OUTER JOIN  
- FULL OUTER JOIN
- ANTI JOIN
- SEMI JOIN
- CROSS JOIN (explicit)

### Advanced Features
- Subqueries in SELECT, FROM, WHERE
- CTEs (Common Table Expressions / WITH clause)
- CASE expressions
- String operations
- Date/time operations
- Type casting
- NULL handling (IS NULL, IS NOT NULL, COALESCE)

### Developer Experience
- Fluent API for star extensions: `col("*").replace(foo="bar").exclude("baz")`
- Type checking (mypy or pyright)
- Better error messages
- Query validation
- SQL formatting/pretty-printing

### Infrastructure
- CI/CD configuration
- Expand CLI functionality beyond basic --version flag
- Documentation site
- Performance benchmarks
- Database driver integration examples
