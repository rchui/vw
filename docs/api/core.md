# Core API Reference

The `vw/core` module provides dialect-agnostic abstractions for building SQL queries.

## Overview

The core API consists of:
- **Expression** - Wrapper for all expression states (columns, operators, functions)
- **RowSet** - Wrapper for sources and statements (tables, queries)
- **Functions** - Factory for aggregate and window functions
- **Frame** - Window frame boundaries for window functions

## Expression

The `Expression` class wraps expression states and provides operators and methods for building SQL expressions.

### Generic Operator

#### `.op(operator, other)`

Create any infix binary operator expression. Useful for database-specific operators not covered by the built-in methods.

```python
col("first_name").op("||", col("last_name"))
# SQL: first_name || last_name
```

### Comparison Operators

All comparison operators delegate to `.op()` internally.

| Operator | SQL | Example |
|----------|-----|---------|
| `==` | `=` | `col("age") == param("age", 18)` |
| `!=` | `<>` | `col("status") != param("s", "deleted")` |
| `<` | `<` | `col("age") < param("max", 65)` |
| `<=` | `<=` | `col("age") <= param("max", 65)` |
| `>` | `>` | `col("price") > param("min", 100)` |
| `>=` | `>=` | `col("price") >= param("min", 100)` |

### Arithmetic Operators

| Operator | SQL | Example |
|----------|-----|---------|
| `+` | `+` | `col("a") + col("b")` |
| `-` | `-` | `col("total") - col("discount")` |
| `*` | `*` | `col("price") * col("quantity")` |
| `/` | `/` | `col("total") / col("count")` |
| `%` | `%` | `col("n") % col("m")` |

### Logical Operators

| Operator | SQL | Example |
|----------|-----|---------|
| `&` | `AND` | `(col("age") >= param("a", 18)) & (col("verified") == param("v", True))` |
| `\|` | `OR` | `(col("status") == param("a", "active")) \| (col("status") == param("p", "pending"))` |
| `~` | `NOT` | `~col("deleted_at").is_null()` |

**Note:** Always use parentheses with logical operators due to Python operator precedence.

### Pattern Matching

#### `.like(pattern)` / `.not_like(pattern)`

```python
col("email").like(param("p", "%@example.com"))
# SQL: email LIKE $p
```

#### `.is_in(*values)` / `.is_not_in(*values)`

```python
col("status").is_in(param("a", "active"), param("p", "pending"))
# SQL: status IN ($a, $p)
```

#### `.between(lower, upper)` / `.not_between(lower, upper)`

```python
col("age").between(param("lo", 18), param("hi", 65))
# SQL: age BETWEEN $lo AND $hi
```

### NULL Checks

#### `.is_null()` / `.is_not_null()`

```python
col("deleted_at").is_null()
# SQL: deleted_at IS NULL
```

### Expression Modifiers

#### `.alias(name)`

```python
(col("price") * col("quantity")).alias("total")
# SQL: price * quantity AS total
```

#### `.cast(data_type)`

```python
col("age").cast("INTEGER")
# SQL: age::INTEGER
```

#### `.asc()` / `.desc()`

```python
col("created_at").desc()
# SQL: created_at DESC
```

### Conditional Expressions

#### `when(condition).then(result)[.when(condition).then(result)...][.otherwise(result) | .end()]`

Build a searched CASE expression. Start with `when()`, chain additional `when().then()` pairs, then finalize with `.otherwise()` (with ELSE) or `.end()` (without ELSE).

```python
from vw.postgres import when

when(col("age") >= param("adult", 18)).then(param("label", "adult"))
    .when(col("age") >= param("teen", 13)).then(param("label", "teen"))
    .otherwise(param("label", "child"))
# SQL: CASE WHEN age >= $adult THEN $label WHEN age >= $teen THEN $label ELSE $label END

when(col("active") == param("t", True)).then(param("yes", 1)).end()
# SQL: CASE WHEN active = $t THEN $yes END
```

CASE expressions are themselves expressions and can be used anywhere an expression is valid: in SELECT, WHERE, ORDER BY, HAVING, and as results of other CASE expressions.

### Window Function Methods

#### `.over(*, partition_by=None, order_by=None)`

Convert function to window function with OVER clause.

```python
F.row_number().over(
    partition_by=[col("category")],
    order_by=[col("price").desc()]
)
# SQL: ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC)
```

#### `.filter(condition)`

Add FILTER (WHERE ...) clause to aggregate or window function.

```python
F.count().filter(col("status") == param("s", "completed"))
# SQL: COUNT(*) FILTER (WHERE status = $s)
```

#### `.rows_between(start, end)` / `.range_between(start, end)`

Add frame clause to window function.

```python
F.sum(col("amount")).over(order_by=[col("date").asc()]).rows_between(UNBOUNDED_PRECEDING, CURRENT_ROW)
# SQL: SUM(amount) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

#### `.exclude(mode)`

Add EXCLUDE clause to window frame. Modes: `"CURRENT ROW"`, `"GROUP"`, `"TIES"`, `"NO OTHERS"`.

---

## RowSet

The `RowSet` class wraps Source or Statement states and provides query building methods.

### `.select(*columns)`

Add columns to SELECT clause. Transforms a bare table reference into a statement.

```python
ref("users").select(col("id"), col("name"))
# SQL: SELECT id, name FROM users
```

### `.where(*conditions)`

Add WHERE clause conditions (accumulated with AND).

```python
ref("users").where(col("age") >= param("a", 18)).where(col("verified") == param("v", True))
# SQL: SELECT * FROM users WHERE age >= $a AND verified = $v
```

### `.group_by(*columns)`

Add GROUP BY clause (replaces previous).

```python
ref("orders").select(col("customer_id"), F.count().alias("n")).group_by(col("customer_id"))
# SQL: SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id
```

### `.having(*conditions)`

Add HAVING clause conditions (accumulated with AND).

```python
.having(F.count() > param("min", 10))
# SQL: HAVING COUNT(*) > $min
```

### `.order_by(*columns)`

Add ORDER BY clause (replaces previous).

```python
.order_by(col("name").asc(), col("id").desc())
# SQL: ORDER BY name ASC, id DESC
```

### `.limit(count, *, offset=None)`

Add LIMIT and optional OFFSET clause.

```python
.limit(10, offset=20)
# SQL: LIMIT 10 OFFSET 20
```

### `.distinct()`

Add DISTINCT clause.

```python
ref("orders").select(col("customer_id")).distinct()
# SQL: SELECT DISTINCT customer_id FROM orders
```

### `.alias(name)`

Alias this rowset (table or subquery).

```python
ref("users").alias("u")
# SQL: users AS u
```

### `.col(name)` / `.star`

Create a qualified column reference using this rowset's alias.

```python
u = ref("users").alias("u")
u.col("id")   # SQL: u.id
u.star        # SQL: u.*
```

### `.join` (property)

Access join operations via `.join.inner()`, `.join.left()`, `.join.right()`, `.join.full_outer()`, `.join.cross()`.

```python
u = ref("users").alias("u")
o = ref("orders").alias("o")
u.join.inner(o, on=[u.col("id") == o.col("user_id")])
# SQL: FROM users AS u INNER JOIN orders AS o ON (u.id = o.user_id)
```

Each join method (except `.cross()`) accepts `on` and/or `using` lists. Multiple joins accumulate in order.

---

## Functions

Import as `F` from the dialect module:

```python
from vw.postgres import F
```

### Aggregate Functions

| Function | SQL |
|----------|-----|
| `F.count()` | `COUNT(*)` |
| `F.count(col("id"))` | `COUNT(id)` |
| `F.count(col("id"), distinct=True)` | `COUNT(DISTINCT id)` |
| `F.sum(col("x"))` | `SUM(x)` |
| `F.avg(col("x"))` | `AVG(x)` |
| `F.min(col("x"))` | `MIN(x)` |
| `F.max(col("x"))` | `MAX(x)` |

### Window Functions

All require `.over()` to add a window specification.

| Function | SQL |
|----------|-----|
| `F.row_number()` | `ROW_NUMBER()` |
| `F.rank()` | `RANK()` |
| `F.dense_rank()` | `DENSE_RANK()` |
| `F.ntile(n)` | `NTILE(n)` |
| `F.lag(col("x"))` | `LAG(x)` |
| `F.lead(col("x"))` | `LEAD(x)` |
| `F.first_value(col("x"))` | `FIRST_VALUE(x)` |
| `F.last_value(col("x"))` | `LAST_VALUE(x)` |

---

## Frame Boundaries

Used with `.rows_between()` and `.range_between()`.

```python
from vw.postgres import UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, preceding, following
```

| Boundary | SQL |
|----------|-----|
| `UNBOUNDED_PRECEDING` | `UNBOUNDED PRECEDING` |
| `UNBOUNDED_FOLLOWING` | `UNBOUNDED FOLLOWING` |
| `CURRENT_ROW` | `CURRENT ROW` |
| `preceding(n)` | `n PRECEDING` |
| `following(n)` | `n FOLLOWING` |

---

## Next Steps

- **[PostgreSQL API](postgres.md)** - PostgreSQL-specific API and examples
- **[Quickstart](../quickstart.md)** - Practical query examples
