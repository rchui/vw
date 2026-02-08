# PostgreSQL API Reference

The `vw.postgres` module provides PostgreSQL-specific implementations and rendering.

## Import

```python
from vw.postgres import ref, col, param, when, exists, cte, render, F
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

### `render(rowset)`

Render a query to SQL. Returns an `SQL` object with `.query` (str) and `.params` (dict).

```python
result = render(query)
print(result.query)   # SQL string with $name placeholders
print(result.params)  # {'name': value, ...}
```

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
