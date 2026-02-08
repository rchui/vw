# PostgreSQL API Reference

The `vw.postgres` module provides PostgreSQL-specific implementations and rendering.

## Import

```python
from vw.postgres import ref, col, param, when, exists, cte, interval, rollup, cube, grouping_sets, render, F
```

## Factory Functions

### `ref(name)`

Create a table or view reference.

```python
users = ref("users")
orders = ref("orders").alias("o")
```

### `col(name)`

Create a column reference. For qualified columns, use `rowset.col(name)`.

```python
col("age")          # age
ref("u").col("id")  # u.id (requires alias set on rowset)
```

### `param(name, value)`

Create a query parameter. Prevents SQL injection.

```python
param("min_age", 18)      # $min_age → {'min_age': 18}
param("status", "active")  # $status → {'status': 'active'}
```

Supported types: `str`, `int`, `float`, `bool`, `None`.

### `when(condition)`

Start a CASE WHEN expression. See [Conditional Expressions](#conditional-expressions).

### `exists(subquery)`

Create an EXISTS check.

```python
exists(ref("orders").where(col("user_id") == col("id")))
# SQL: EXISTS (SELECT * FROM orders WHERE user_id = id)
```

### `cte(name, query, *, recursive=False)`

Create a Common Table Expression (WITH clause).

```python
active = cte("active", ref("users").select(col("*")).where(col("active") == param("t", True)))
result = active.select(col("id"), col("name"))
# SQL: WITH active AS (SELECT * FROM users WHERE active = $t) SELECT id, name FROM active
```

### `values(alias, *rows)`

Create a VALUES clause as an inline row source. The alias is required.

```python
values("t", {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"})
    .select(col("id"), col("name"))
# SQL: SELECT id, name FROM (VALUES ($1, $2), ($3, $4)) AS t(id, name)
```

Row values can be Python literals (parameterized automatically) or expressions:

```python
values("t", {"id": 1, "ts": param("now", "NOW()")}).select(col("*"))
# SQL: SELECT * FROM (VALUES ($1, $now)) AS t(id, ts)
```

### `rollup(*columns)`

Create a ROLLUP grouping construct for hierarchical subtotals.

```python
ref("sales").select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
    .group_by(rollup(col("region"), col("product")))
# SQL: SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP (region, product)
```

### `cube(*columns)`

Create a CUBE grouping construct for all combinations of dimensions.

```python
ref("sales").select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
    .group_by(cube(col("region"), col("product")))
# SQL: SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE (region, product)
```

### `grouping_sets(*sets)`

Create a GROUPING SETS construct for explicit grouping combinations. Use `()` (empty tuple) for the grand total row.

```python
ref("sales").select(col("region"), col("product"), F.sum(col("amount")).alias("total"))
    .group_by(grouping_sets(
        (col("region"), col("product")),
        (col("region"),),
        (),
    ))
# SQL: SELECT region, product, SUM(amount) AS total
#      FROM sales
#      GROUP BY GROUPING SETS ((region, product), (region), ())
```

### `interval(amount, unit)`

Create a PostgreSQL `INTERVAL` literal. Use with `+` and `-` for date arithmetic.

```python
interval(1, "day")      # INTERVAL '1 day'
interval(30, "day")     # INTERVAL '30 day'
interval(1.5, "hour")   # INTERVAL '1.5 hour'

col("created_at") + interval(1, "day")   # created_at + INTERVAL '1 day'
col("expires_at") - interval(30, "day")  # expires_at - INTERVAL '30 day'
```

### `render(rowset)`

Render a query to SQL. Returns an `SQL` object with `.query` (str) and `.params` (dict).

```python
result = render(query)
print(result.query)   # SQL string with $name placeholders
print(result.params)  # {'name': value, ...}
```

---

## Types

`vw.postgres.types` provides type functions for use with `.cast()`. Each function returns the SQL type string. All core types are re-exported, plus PostgreSQL-specific additions.

```python
from vw.postgres import types

col("id").cast(types.INTEGER())          # id::INTEGER
col("name").cast(types.VARCHAR(255))     # name::VARCHAR(255)
col("price").cast(types.NUMERIC(10, 2)) # price::NUMERIC(10,2)
col("ts").cast(types.TIMESTAMPTZ())      # ts::TIMESTAMPTZ
col("data").cast(types.JSONB())          # data::JSONB
```

### PostgreSQL-specific types

| Function | SQL |
|----------|-----|
| `types.TIMESTAMPTZ()` | `TIMESTAMPTZ` |
| `types.JSONB()` | `JSONB` |

For all core types (`INTEGER`, `VARCHAR`, `NUMERIC`, etc.) see [core types](core.md#cast).

---

## Date/Time

### Grouping Functions

| Function | SQL |
|----------|-----|
| `F.grouping(col("x"), ...)` | `GROUPING(x, ...)` |

Use `F.grouping()` in SELECT to identify which columns are aggregated in each grouping set row.

```python
ref("sales").select(
    col("region"),
    F.sum(col("amount")).alias("total"),
    F.grouping(col("region")).alias("is_grand_total"),
).group_by(grouping_sets((col("region"),), ()))
```

### PostgreSQL-specific functions

| Function | SQL |
|----------|-----|
| `F.now()` | `NOW()` |

### `.dt.date_trunc(unit)`

Truncate a timestamp to the specified precision (PostgreSQL-specific).

```python
col("created_at").dt.date_trunc("month")  # DATE_TRUNC('month', created_at)
col("ts").dt.date_trunc("year")           # DATE_TRUNC('year', ts)
```

For ANSI SQL `EXTRACT`, see [Date/Time Accessor](core.md#datetime-accessor) in the core docs.

---

## Conditional Expressions

Build searched CASE expressions with the `when()` builder:

```python
from vw.postgres import when, col, param

# With ELSE
when(col("age") >= param("adult", 18)).then(param("a", "adult"))
    .when(col("age") >= param("teen", 13)).then(param("t", "teen"))
    .otherwise(param("c", "child"))
# SQL: CASE WHEN age >= $adult THEN $a WHEN age >= $teen THEN $t ELSE $c END

# Without ELSE (returns NULL when no branch matches)
when(col("vip") == param("t", True)).then(param("label", "VIP")).end()
# SQL: CASE WHEN vip = $t THEN $label END
```

CASE expressions can be used anywhere an expression is valid — SELECT columns, WHERE conditions, ORDER BY, HAVING, and nested inside other CASE expressions.

```python
# As a SELECT column with alias
ref("users").select(
    col("id"),
    when(col("active") == param("t", True)).then(param("y", "yes")).otherwise(param("n", "no"))
    .alias("is_active")
)

# Nested CASE
inner = when(col("score") >= param("hi", 90)).then(param("g", "gold")).otherwise(param("s", "silver"))
when(col("tier") == param("p", "premium")).then(inner).otherwise(param("b", "bronze"))
```

---

## Set Operations

Combine queries with operator overloading:

| Operator | SQL |
|----------|-----|
| `q1 \| q2` | `UNION` |
| `q1 + q2` | `UNION ALL` |
| `q1 & q2` | `INTERSECT` |
| `q1 - q2` | `EXCEPT` |

```python
users = ref("users").select(col("id"))
admins = ref("admins").select(col("id"))

render(users | admins)   # (SELECT id FROM users) UNION (SELECT id FROM admins)
render(users + admins)   # (SELECT id FROM users) UNION ALL (SELECT id FROM admins)
```

---

## Parameter Style

PostgreSQL rendering uses **dollar-style** parameters: `$name`.

Compatible with SQLAlchemy `text()`, asyncpg, and psycopg3.

---

## Complete Examples

### Basic Query

```python
from vw.postgres import ref, col, param, render

result = render(
    ref("users")
    .select(col("id"), col("name"), col("email"))
    .where(col("active") == param("active", True))
    .order_by(col("name").asc())
    .limit(10)
)
```

### Aggregation

```python
from vw.postgres import ref, col, param, render, F

result = render(
    ref("orders")
    .select(
        col("customer_id"),
        F.count().alias("order_count"),
        F.sum(col("amount")).alias("total_spent"),
    )
    .where(col("created_at") >= param("since", "2024-01-01"))
    .group_by(col("customer_id"))
    .having(F.sum(col("amount")) > param("min", 1000))
    .order_by(F.sum(col("amount")).desc())
)
```

### Window Functions

```python
from vw.postgres import ref, col, render, F

result = render(
    ref("sales").select(
        col("product_id"),
        col("amount"),
        F.row_number().over(
            partition_by=[col("product_id")],
            order_by=[col("sale_date").desc()]
        ).alias("rank"),
        F.sum(col("amount")).over(
            order_by=[col("sale_date").asc()]
        ).alias("running_total"),
    )
)
```

### Joins

```python
from vw.postgres import ref, col, param, render, F

u = ref("users").alias("u")
o = ref("orders").alias("o")

result = render(
    u.join.inner(o, on=[u.col("id") == o.col("user_id")])
    .select(u.col("name"), F.count().alias("order_count"))
    .group_by(u.col("id"), u.col("name"))
)
```

### CASE Expression

```python
from vw.postgres import ref, col, param, render, when

result = render(
    ref("orders").select(
        col("id"),
        when(col("status") == param("s1", "pending")).then(param("r1", 1))
        .when(col("status") == param("s2", "shipped")).then(param("r2", 2))
        .otherwise(param("r3", 0))
        .alias("status_code"),
    )
)
```

### Subquery

```python
from vw.postgres import ref, col, param, render

active_users = (
    ref("users")
    .select(col("id"), col("name"))
    .where(col("status") == param("s", "active"))
    .alias("active_users")
)
result = render(active_users.select(col("name")).limit(10))
```

### CTE

```python
from vw.postgres import ref, col, param, render, cte

active = cte("active", ref("users").select(col("*")).where(col("active") == param("t", True)))
result = render(active.select(col("id"), col("name")))
```

### Using with SQLAlchemy

```python
from sqlalchemy import create_engine, text
from vw.postgres import ref, col, param, render

engine = create_engine("postgresql://user:pass@localhost/mydb")
result = render(ref("users").select(col("id")).where(col("age") >= param("min", 18)))

with engine.connect() as conn:
    rows = conn.execute(text(result.query), result.params)
```

---

## Feature Status

See [PostgreSQL Parity](../development/postgres-parity.md) for the full roadmap.

**Completed:**
- Core query building (SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT)
- Operators (comparison, arithmetic, logical, pattern matching, NULL checks)
- Aggregate and window functions with FILTER and frame clauses
- Joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- Subqueries (FROM, WHERE with EXISTS/IN)
- Set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
- CTEs (WITH, WITH RECURSIVE)
- Conditional expressions (CASE WHEN)
- Parameters and rendering
- Scalar functions: string (.text), null handling, date/time (.dt)
- Grouping constructs (ROLLUP, CUBE, GROUPING SETS) and GROUPING() function
