# vw Documentation

**SQL, made composable.**

vw is a lightweight Python library that brings Polars-inspired method chaining to the world of SQL. Build complex queries with fluent, composable building blocks while maintaining the full power of SQL.

## Why vw?

- **Love SQL**: No abstractions that hide the engine. Write real SQL logic, just more cleanly.
- **Lose the Fear**: Break "monster queries" into small, testable, and chainable methods.
- **Polars-Inspired**: If you can use a data frame, you can build a query.
- **Type-Safe**: Leverage Python's type system for safer query construction.
- **Dialect-Aware**: Built-in support for PostgreSQL and DuckDB (coming soon).

## Quick Example

```python
from vw.postgres import source, col, param, render

users = source("users")
query = (
    users
    .select(col("id"), col("name"), col("email"))
    .where(col("age") >= param("min_age", 18))
    .where(col("status") == param("status", "active"))
    .order_by(col("name").asc())
    .limit(10)
)

result = render(query)
# SQL: SELECT id, name, email FROM users
#      WHERE age >= $min_age AND status = $status
#      ORDER BY name ASC LIMIT 10
# Params: {'min_age': 18, 'status': 'active'}
```

## Features

### Completed (PostgreSQL)
- ✅ Core query building (SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
- ✅ Column references (qualified/unqualified, star expressions, aliasing)
- ✅ Operators (comparison, arithmetic, logical, pattern matching, NULL checks)
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- ✅ Window frames (ROWS/RANGE BETWEEN, frame exclusion)
- ✅ FILTER clause for aggregates and window functions
- ✅ Parameters and parameter binding

### In Progress
- 🚧 Joins (INNER, LEFT, RIGHT, FULL, CROSS)
- 🚧 Subqueries and CTEs
- 🚧 Set operations (UNION, INTERSECT, EXCEPT)
- 🚧 Scalar functions (string, datetime, null handling)
- 🚧 DuckDB dialect support

### Planned
- 📋 DML statements (INSERT, UPDATE, DELETE)
- 📋 DDL statements (CREATE TABLE, CREATE VIEW, etc.)
- 📋 PostgreSQL-specific features (DISTINCT ON, JSONB, arrays)
- 📋 DuckDB-specific features (star extensions, file I/O)

## Documentation

- **[Getting Started](getting-started.md)** - Introduction and core concepts
- **[Installation](installation.md)** - Installation instructions
- **[Quickstart](quickstart.md)** - Quick examples to get you started
- **[Architecture](architecture.md)** - How vw is structured

### API Reference
- **[API Overview](api/index.md)** - Overview of the API
- **[Core API](api/core.md)** - Dialect-agnostic core
- **[PostgreSQL API](api/postgres.md)** - PostgreSQL-specific API
- **[DuckDB API](api/duckdb.md)** - DuckDB-specific API

### Development
- **[PostgreSQL Parity](development/postgres-parity.md)** - Feature roadmap for PostgreSQL
- **[DuckDB Parity](development/duckdb-parity.md)** - Feature roadmap for DuckDB

## Project Philosophy

vw is designed with these principles:

1. **SQL is powerful** - Don't hide it, embrace it
2. **Composition over configuration** - Build queries from reusable parts
3. **Type safety** - Let Python's type system catch errors early
4. **Explicit over implicit** - No magic, no surprises
5. **Immutability** - Query building never mutates, always creates new objects

## License

MIT

## Acknowledgments

This project was inspired by [Toad](https://willmcgugan.github.io/toad-released/) and built with the philosophy that SQL doesn't need to be hidden behind heavy abstractions - it just needs better tooling.
