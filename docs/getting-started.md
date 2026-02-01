# Getting Started

## What is vw?

vw (pronounced "view") is a SQL query builder that brings method chaining to SQL. Instead of concatenating strings or learning a heavy ORM, you build queries using fluent, chainable methods that feel like working with a Polars DataFrame.

## Core Concepts

### 1. Immutable State

Every query operation returns a new object. Nothing is mutated:

```python
from vw.postgres import source, col

users = source("users")
query1 = users.select(col("id"))
query2 = query1.where(col("age") > 18)

# query1 is unchanged - still just SELECT id FROM users
# query2 is SELECT id FROM users WHERE age > 18
```

### 2. Method Chaining

Build queries by chaining methods together:

```python
query = (
    users
    .select(col("name"), col("email"))
    .where(col("active") == True)
    .order_by(col("name").asc())
    .limit(10)
)
```

### 3. Expressions

Columns and parameters are expressions that support operators:

```python
# Comparison
col("age") >= 18
col("status") == "active"

# Arithmetic
col("price") * col("quantity")
col("total") - col("discount")

# Logical
(col("age") >= 18) & (col("verified") == True)

# Pattern matching
col("email").like("%@example.com")
col("id").is_in(1, 2, 3, 4, 5)
```

### 4. Parameters

Always use parameters for values, never concatenate strings:

```python
from vw.postgres import param

# Good - uses parameters
query = users.where(col("age") >= param("min_age", 18))

# Bad - don't do this
# query = users.where(f"age >= {min_age}")  # SQL injection risk!
```

### 5. Rendering

Convert the query to SQL and parameters:

```python
from vw.postgres import render

result = render(query)
print(result.query)   # The SQL string
print(result.params)  # The parameters dict
```

## Basic Workflow

1. **Import** - Import from the dialect module you need:
   ```python
   from vw.postgres import source, col, param, render
   ```

2. **Create a source** - Start with a table or view:
   ```python
   users = source("users")
   ```

3. **Build the query** - Chain methods to build your query:
   ```python
   query = (
       users
       .select(col("id"), col("name"))
       .where(col("active") == param("active", True))
   )
   ```

4. **Render** - Convert to SQL:
   ```python
   result = render(query)
   ```

5. **Execute** - Use with your database driver:
   ```python
   from sqlalchemy import create_engine, text

   engine = create_engine("postgresql://...")
   with engine.connect() as conn:
       rows = conn.execute(text(result.query), result.params)
   ```

## Why Use vw?

### Problem: String Concatenation is Fragile

```python
# Fragile and hard to maintain
def get_users(active=None, min_age=None, max_results=None):
    sql = "SELECT * FROM users WHERE 1=1"
    params = {}

    if active is not None:
        sql += " AND active = :active"
        params['active'] = active

    if min_age is not None:
        sql += " AND age >= :min_age"
        params['min_age'] = min_age

    if max_results is not None:
        sql += " LIMIT :limit"
        params['limit'] = max_results

    return sql, params
```

### Solution: Composable Query Building

```python
# Clean and composable
def get_users(active=None, min_age=None, max_results=None):
    query = source("users").select(col("*"))

    if active is not None:
        query = query.where(col("active") == param("active", active))

    if min_age is not None:
        query = query.where(col("age") >= param("min_age", min_age))

    if max_results is not None:
        query = query.limit(max_results)

    return render(query)
```

## Next Steps

- **[Installation](installation.md)** - Install vw
- **[Quickstart](quickstart.md)** - See more examples
- **[Architecture](architecture.md)** - Understand how vw works
- **[API Reference](api/index.md)** - Detailed API documentation
