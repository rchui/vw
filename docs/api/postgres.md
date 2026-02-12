# PostgreSQL API Reference

The `vw.postgres` module provides PostgreSQL-specific implementations and rendering.

## Import

```python
from vw.postgres import ref, col, param, lit, when, exists, cte, interval, rollup, cube, grouping_sets, raw, render, F
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

### `lit(value)`

Create a literal value expression. Literals are compile-time constants rendered directly in SQL with proper escaping for SQL injection safety.

```python
# Use lit() for constants in SQL
F.string_agg(col("name"), lit(", "))  # STRING_AGG(name, ', ')
F.json_build_object(lit("id"), col("id"), lit("name"), col("name"))  # JSON_BUILD_OBJECT('id', id, 'name', name)
col("status") == lit("active")  # status = 'active'
col("priority") > lit(5)  # priority > 5

# Use param() for user input (self-documenting)
col("age") > param("min_age", 18)  # age > $min_age
```

Literal rendering:
- Strings: quoted and escaped (`'active'`, `'user''s choice'`)
- Numbers: rendered as-is (`42`, `19.99`)
- Booleans: `TRUE` / `FALSE`
- None: `NULL`

Use `lit()` for: JSON keys, separators, status strings, magic numbers.
Use `param()` for: user input, runtime values (self-documenting).

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

## PostgreSQL Convenience Functions

High-value convenience wrappers for extremely common PostgreSQL functions. For other PostgreSQL functions, use `raw.func()` or `raw.expr()`.

### UUID Functions

#### `F.gen_random_uuid()`

Generate a random UUID v4 (requires pgcrypto extension or PostgreSQL 13+).

```python
ref("users").select(F.gen_random_uuid().alias("id"), col("name"))
# SQL: SELECT GEN_RANDOM_UUID() AS id, name FROM users
```

### Array Aggregate Functions

#### `F.array_agg(expr, *, distinct=False, order_by=None)`

Aggregate values into an array. Supports DISTINCT and ORDER BY inside the function.

```python
# Basic usage
F.array_agg(col("name"))
# SQL: ARRAY_AGG(name)

# With ORDER BY
F.array_agg(col("name"), order_by=[col("name").asc()])
# SQL: ARRAY_AGG(name ORDER BY name ASC)

# With DISTINCT
F.array_agg(col("tag"), distinct=True)
# SQL: ARRAY_AGG(DISTINCT tag)

# Combined
F.array_agg(col("tag"), distinct=True, order_by=[col("tag")])
# SQL: ARRAY_AGG(DISTINCT tag ORDER BY tag)

# With GROUP BY
ref("posts").select(
    col("user_id"),
    F.array_agg(col("tag"), order_by=[col("tag")]).alias("tags")
).group_by(col("user_id"))
```

#### `F.string_agg(expr, separator, *, order_by=None)`

Concatenate values into a string with a separator. Supports ORDER BY inside the function.

```python
# Basic usage
F.string_agg(col("name"), lit(", "))
# SQL: STRING_AGG(name, $_lit_0)  -- separator as literal parameter

# With ORDER BY
F.string_agg(col("name"), lit(", "), order_by=[col("name")])
# SQL: STRING_AGG(name, $_lit_0 ORDER BY name)

# With GROUP BY
ref("users").select(
    col("department"),
    F.string_agg(col("name"), lit(", "), order_by=[col("name")]).alias("names")
).group_by(col("department"))
```

### JSON Functions

#### `F.json_build_object(*args)`

Build a JSON object from alternating key/value pairs. Use `lit()` for string keys.

```python
# Basic usage
F.json_build_object(lit("id"), col("id"), lit("name"), col("name"))
# SQL: JSON_BUILD_OBJECT($_lit_0, id, $_lit_1, name)

# Many fields
F.json_build_object(
    lit("id"), col("id"),
    lit("name"), col("name"),
    lit("email"), col("email"),
    lit("status"), col("status")
)
```

#### `F.json_agg(expr, *, order_by=None)`

Aggregate values into a JSON array. Supports ORDER BY inside the function.

```python
# Basic usage
F.json_agg(col("data"))
# SQL: JSON_AGG(data)

# With ORDER BY
F.json_agg(col("data"), order_by=[col("created_at")])
# SQL: JSON_AGG(data ORDER BY created_at)

# Wrapping json_build_object
json_obj = F.json_build_object(lit("id"), col("id"), lit("name"), col("name"))
F.json_agg(json_obj, order_by=[col("name")])
# SQL: JSON_AGG(JSON_BUILD_OBJECT($_lit_0, id, $_lit_1, name) ORDER BY name)
```

### Array Functions

#### `F.unnest(array)`

Expand an array to a set of rows. Can be used in SELECT clause.

```python
# In SELECT
ref("posts").select(F.unnest(col("tags")).alias("tag"))
# SQL: SELECT UNNEST(tags) AS tag FROM posts

# For FROM clause usage, use raw.rowset()
raw.rowset("unnest({arr}) AS t(elem)", arr=col("array_col"))
```

### Combining with FILTER

All aggregate functions support the `.filter()` method for conditional aggregation:

```python
# array_agg with FILTER and ORDER BY
F.array_agg(col("name"), order_by=[col("name")]).filter(col("active") == param("t", True))
# SQL: ARRAY_AGG(name ORDER BY name) FILTER (WHERE active = $t)

# string_agg with FILTER and ORDER BY
F.string_agg(col("name"), lit(", "), order_by=[col("name")]).filter(col("active") == param("t", True))
# SQL: STRING_AGG(name, $_lit_0 ORDER BY name) FILTER (WHERE active = $t)
```

---

## Raw SQL API

For PostgreSQL features not yet wrapped by vw, use the raw SQL API:

### `raw.func(name, *args)`

Convenience method for simple function calls. For functions with special syntax (WITHIN GROUP, complex ORDER BY), use `raw.expr()` instead.

```python
# Zero arguments
raw.func("random")
# SQL: RANDOM()

# With arguments
raw.func("custom_hash", col("email"))
# SQL: CUSTOM_HASH(email)

# Multiple arguments
raw.func("my_func", col("a"), col("b"), param("c", 42))
# SQL: MY_FUNC(a, b, $c)
```

Function names are automatically uppercased.

### `raw.expr(template, **kwargs)` and `raw.rowset(template, **kwargs)`

For complex PostgreSQL expressions and table functions. See [Raw SQL API](../development/postgres-parity.md#raw-sql-api-enhancements) for full documentation.

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

## Query Modifiers

### `.offset(count)`

Skip the first `count` rows from the result set. Use with `.order_by()` for deterministic pagination.

```python
ref("users").select(col("id"), col("name"))
    .order_by(col("id"))
    .offset(20)
    .limit(10)
# SQL: SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20
```

**Migration Note:** In earlier versions, offset was a parameter of `.limit(count, offset=n)`. This has been changed to a separate method for better composability:

```python
# Old (deprecated):
query.limit(10, offset=20)

# New:
query.offset(20).limit(10)
```

### `.fetch(count, *, with_ties=False)`

Use SQL standard `FETCH FIRST` clause for pagination. An alternative to `LIMIT` with additional support for `WITH TIES` to include rows that tie with the last row in the ordering.

```python
# Basic fetch
ref("users").select(col("id"), col("name"))
    .order_by(col("created_at"))
    .fetch(10)
# SQL: SELECT id, name FROM users ORDER BY created_at FETCH FIRST 10 ROWS ONLY

# With ties - include all rows that tie with the last row
ref("leaderboard").select(col("player"), col("score"))
    .order_by(col("score").desc())
    .fetch(5, with_ties=True)
# SQL: SELECT player, score FROM leaderboard ORDER BY score DESC FETCH FIRST 5 ROWS WITH TIES

# Combine with offset
ref("products").select(col("id"), col("name"))
    .order_by(col("name"))
    .offset(20)
    .fetch(10)
# SQL: SELECT id, name FROM products ORDER BY name OFFSET 20 FETCH FIRST 10 ROWS ONLY
```

### `.modifiers(*modifiers)`

Add modifiers to queries or tables. Modifiers are rendered after the main query (for statement-level modifiers) or after the table name (for table-level modifiers).

Common use cases:
- **Row-level locking**: `FOR UPDATE`, `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`
- **Table sampling**: `TABLESAMPLE SYSTEM(n)`, `TABLESAMPLE BERNOULLI(n)`
- **Lock options**: `SKIP LOCKED`, `NOWAIT`

⚠️ **Security Warning:** Modifiers use `raw.expr()` which has no SQL injection protection. Never use string concatenation or f-strings with user input. Always use `param()` for dynamic values.

**Statement-level modifiers:**

```python
from vw.postgres import raw, ref, col, param, render

# Row-level locking for updates
ref("accounts").select(col("*"))
    .where(col("id") == param("account_id", 123))
    .modifiers(raw.expr("FOR UPDATE"))
# SQL: SELECT * FROM accounts WHERE id = $account_id FOR UPDATE

# Job queue pattern - skip locked rows
ref("jobs").select(col("*"))
    .where(col("status") == param("status", "pending"))
    .order_by(col("priority").desc())
    .limit(1)
    .modifiers(raw.expr("FOR UPDATE SKIP LOCKED"))
# SQL: SELECT * FROM jobs WHERE status = $status ORDER BY priority DESC LIMIT 1 FOR UPDATE SKIP LOCKED

# Shared lock with nowait
ref("users").select(col("*"))
    .where(col("active"))
    .modifiers(raw.expr("FOR SHARE NOWAIT"))
# SQL: SELECT * FROM users WHERE active FOR SHARE NOWAIT
```

**Table-level modifiers:**

```python
# Random sampling
ref("events").modifiers(raw.expr("TABLESAMPLE SYSTEM(5)"))
    .select(col("*"))
    .where(col("event_type") == param("event_type", "click"))
# SQL: SELECT * FROM events TABLESAMPLE SYSTEM(5) WHERE event_type = $event_type

# Partition selection (if table is partitioned)
ref("logs").modifiers(raw.expr("PARTITION (p2024_01)"))
    .select(col("*"))
    .where(col("level") == param("level", "ERROR"))
# SQL: SELECT * FROM logs PARTITION (p2024_01) WHERE level = $level
```

**Multiple modifiers:**

Modifiers accumulate - you can call `.modifiers()` multiple times or pass multiple modifiers:

```python
# Multiple calls accumulate
query = ref("users").select(col("*"))
query = query.modifiers(raw.expr("FOR UPDATE"))
query = query.modifiers(raw.expr("SKIP LOCKED"))
# SQL: SELECT * FROM users FOR UPDATE SKIP LOCKED

# Or pass multiple modifiers at once
query = ref("users").select(col("*"))
    .modifiers(raw.expr("FOR UPDATE"), raw.expr("SKIP LOCKED"))
# SQL: SELECT * FROM users FOR UPDATE SKIP LOCKED
```

---

## Parameter Style

PostgreSQL rendering uses **dollar-style** parameters: `$name`.

Compatible with SQLAlchemy `text()`, asyncpg, and psycopg3.

---

## Raw SQL Escape Hatches

⚠️  **WARNING**: The `raw` namespace provides escape hatches for SQL features that vw doesn't support yet. Use with caution:
- **No syntax validation** until query execution
- **No SQL injection protection** if you concatenate strings into templates
- **No type checking**
- **No dialect portability**

**Best practices:**
- Only use for features vw doesn't support yet
- Always pass user input via `param()`, never f-strings or string concatenation
- Use `{name}` placeholders for dependent expressions
- Consider filing a feature request for native support
- Add comments explaining why raw SQL is needed

### `raw.expr(template, **kwargs)`

Create a raw SQL expression with named parameter substitution.

Use `{name}` placeholders in the template. Each kwarg provides an expression to substitute for that placeholder. Rendering happens at render time to support context-dependent features like CTEs.

**Args:**
- `template`: Raw SQL template string with `{name}` placeholders
- `**kwargs`: Named expressions to substitute into the template

**Returns:** Expression that can be used anywhere expressions are expected

**Examples:**

```python
from vw.postgres import raw, col, param, ref, render

# PostgreSQL-specific operators (alternatively, use .op() method)
query = ref("posts").where(
    raw.expr("{a} @@ {b}", a=col("tsv"), b=col("query"))
)

# Or use the existing .op() method for custom operators:
query = ref("posts").where(
    col("tsv").op("@@", col("query"))
)

# Custom functions
query = ref("sales").select(
    raw.expr("percentile_cont({p}) WITHIN GROUP (ORDER BY {amt})",
             p=param("pct", 0.95),
             amt=col("amount")).alias("p95")
)

# Complex expressions
query = ref("posts").select(
    raw.expr("ts_rank({tsv}, {q})", tsv=col("tsv"), q=col("query")).alias("rank")
)

# Use in join conditions
buildings = ref("buildings").alias("b")
parcels = ref("parcels").alias("p")
query = buildings.join.inner(
    parcels,
    on=[raw.expr("ST_Within({a}, {b})", a=col("b.geom"), b=col("p.geom"))]
)
```

### `raw.rowset(template, **kwargs)`

Create a raw SQL source/rowset with named parameter substitution.

Use `{name}` placeholders in the template. Each kwarg provides an expression to substitute for that placeholder. Rendering happens at render time to support context-dependent features like CTEs.

**Args:**
- `template`: Raw SQL source template string with `{name}` placeholders
- `**kwargs`: Named expressions to substitute into the template

**Returns:** RowSet that can be used in FROM, JOIN, or CTE contexts

**Examples:**

```python
from vw.postgres import raw, col, param, render

# PostgreSQL table function
series = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
result = render(series.select(col("num")))
# SQL: SELECT num FROM generate_series(1, $max) AS t(num)

# LATERAL with unnest
query = ref("arrays").join.inner(
    raw.rowset("LATERAL unnest({arr}) AS t(elem)", arr=col("array_col")),
    on=[]
)

# JSON table function
query = raw.rowset(
    "json_to_recordset({data}) AS t(id INT, name TEXT)",
    data=col("json_data")
).select(col("id"), col("name"))
```

**Safety example:**

```python
# ✅ Good: Using param() for user input
user_input = request.get("value")
expr = raw.expr("custom_func({input})", input=param("val", user_input))
# SQL: custom_func($val)
# Params: {'val': user_input}

# ❌ Bad: String concatenation (SQL INJECTION RISK!)
expr = raw.expr(f"custom_func({user_input})")  # NEVER DO THIS!
```

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

### LATERAL Joins

LATERAL joins allow the right side of a join to reference columns from the left side, enabling correlated subqueries in the FROM clause. This is useful for:
- Computing values that depend on earlier tables in the join
- Getting top-N records per group efficiently
- Using set-returning functions with correlation

```python
from vw.postgres import ref, col, render, F

users = ref("users").alias("u")
orders = ref("orders")

# Get top 3 most recent orders per user
recent_orders = (
    orders
    .select(orders.col("id"), orders.col("total"), orders.col("created_at"))
    .where(orders.col("user_id") == users.col("id"))  # Correlated with outer query
    .order_by(orders.col("created_at").desc())
    .limit(3)
    .alias("recent")
)

result = render(
    users
    .join.left(recent_orders, on=[col("TRUE")], lateral=True)
    .select(users.col("name"), recent_orders.col("total"))
)
# LEFT JOIN LATERAL (SELECT ...) AS recent ON (TRUE)

# CROSS JOIN LATERAL with set-returning function
series = ref("generate_series(1, 5)").alias("n")
result = render(
    users
    .join.cross(series, lateral=True)
    .select(users.col("name"), col("n"))
)
# CROSS JOIN LATERAL generate_series(1, 5) AS n
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
- Query modifiers (FETCH, row-level locking, table sampling via .modifiers())
