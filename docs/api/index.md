# API Reference

vw provides a clean, composable API for building SQL queries.

## Module Structure

```
vw/
├── core/         → Dialect-agnostic abstractions
├── postgres/     → PostgreSQL-specific implementation
└── duckdb/       → DuckDB-specific implementation (incomplete)
```

## Quick Reference

### PostgreSQL

```python
from vw.postgres as vw, col, param, render, F

# Factory functions
users = source("users")              # Create table source
age = col("age")                     # Create column reference
min_age = param("min_age", 18)       # Create parameter

# Build query
query = (
    users
    .select(col("id"), col("name"))
    .where(col("age") >= min_age)
)

# Render to SQL
result = render(query)               # Returns SQL(query=..., params=...)

# Aggregate functions
F.count()                            # COUNT(*)
F.sum(col("amount"))                 # SUM(amount)
F.avg(col("price"))                  # AVG(price)

# Window functions
F.row_number().over(...)             # ROW_NUMBER() OVER (...)
F.rank().over(...)                   # RANK() OVER (...)
```

### DuckDB (Incomplete)

```python
from vw.duckdb import source

# Currently only basic source creation is available
users = source("users")
```

## API Documentation

- **[Core API](core.md)** - Dialect-agnostic core (Expression, RowSet, States)
- **[PostgreSQL API](postgres.md)** - PostgreSQL-specific API
- **[DuckDB API](duckdb.md)** - DuckDB-specific API

## Common Patterns

### Creating Sources

```python
# Simple table
users = source("users")

# Aliased table
users = source("users").alias("u")

# Subquery as source
active_users = (
    source("users")
    .select(col("id"), col("name"))
    .where(col("status") == "active")
    .alias("active_users")
)
```

### Building WHERE Clauses

```python
# Single condition
query = users.where(col("age") >= 18)

# Multiple conditions (AND)
query = users.where(col("age") >= 18).where(col("verified") == True)

# Complex conditions with operators
query = users.where(
    ((col("age") >= 18) & (col("verified") == True)) |
    (col("admin") == True)
)
```

### Selecting Columns

```python
# Specific columns
query = users.select(col("id"), col("name"))

# All columns
query = users.select(col("*"))

# Qualified columns
query = users.select(users.col("id"), users.col("name"))

# With aliases
query = users.select(
    col("id").alias("user_id"),
    col("name").alias("full_name")
)

# Expressions
query = users.select(
    col("id"),
    (col("first_name") + col("last_name")).alias("full_name")
)
```

### Aggregation

```python
# Simple aggregation
query = orders.select(
    F.count().alias("total"),
    F.sum(col("amount")).alias("revenue")
)

# With GROUP BY
query = (
    orders
    .select(
        col("customer_id"),
        F.count().alias("order_count")
    )
    .group_by(col("customer_id"))
)

# With HAVING
query = (
    orders
    .select(col("customer_id"), F.count().alias("cnt"))
    .group_by(col("customer_id"))
    .having(F.count() > 10)
)
```

### Window Functions

```python
# Basic window function
query = sales.select(
    col("*"),
    F.row_number().over(
        order_by=[col("sale_date").desc()]
    ).alias("row_num")
)

# With PARTITION BY
query = sales.select(
    col("*"),
    F.rank().over(
        partition_by=[col("product_id")],
        order_by=[col("amount").desc()]
    ).alias("rank")
)

# With window frame
from vw.core.frame import UNBOUNDED_PRECEDING, CURRENT_ROW

query = sales.select(
    col("*"),
    F.sum(col("amount")).over(
        order_by=[col("date").asc()]
    ).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW).alias("running_total")
)
```

### Ordering and Limiting

```python
# ORDER BY
query = users.select(col("*")).order_by(col("name").asc())

# Multiple columns
query = users.select(col("*")).order_by(
    col("last_name").asc(),
    col("first_name").asc()
)

# LIMIT
query = users.select(col("*")).limit(10)

# LIMIT with OFFSET
query = users.select(col("*")).offset(20).limit(10)
```

## Type Reference

### SQL Result

```python
from vw.core.render import SQL

@dataclass
class SQL:
    query: str        # The SQL query string
    params: dict      # Parameters dictionary
```

### Parameter Styles

```python
from vw.core.render import ParamStyle

class ParamStyle(Enum):
    COLON = "colon"          # :name
    DOLLAR = "dollar"        # $name (PostgreSQL default)
    AT = "at"                # @name
    PYFORMAT = "pyformat"    # %(name)s
```

## Next Steps

- **[Core API](core.md)** - Detailed core API reference
- **[PostgreSQL API](postgres.md)** - PostgreSQL-specific reference
- **[DuckDB API](duckdb.md)** - DuckDB-specific reference
