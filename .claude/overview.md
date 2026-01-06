# VW Project Overview

## What is VW?

**vw** is a SQL builder library that uses polars-inspired method chaining syntax for constructing SQL queries with parameterized values.

### Purpose
- Build SQL queries using method chaining (e.g., `Source(name="users").select(col("*")).render()`)
- Support parameterized queries for security and performance
- Inspired by polars syntax for a fluent, readable API
- Type-safe query construction with Python protocols

## Project Structure

```
vw/
├── .claude/              # Project documentation
│   ├── overview.md       # This file - project overview
│   ├── setup.md          # Installation and configuration
│   ├── api.md            # API reference and usage
│   ├── architecture.md   # Design decisions and architecture
│   ├── testing.md        # Testing conventions
│   └── features.md       # Current features and roadmap
├── .gitignore
├── .venv/                # Virtual environment (uv)
├── README.md
├── pyproject.toml        # Project configuration
├── vw/                   # Main package directory
│   ├── __init__.py       # Package init, exports public API
│   ├── cli.py            # CLI entry point
│   ├── expr.py           # Expression classes (Column, Parameter, etc.)
│   ├── query.py          # Query builder classes (Source, Statement)
│   └── render.py         # Rendering infrastructure (RenderContext, RenderConfig, etc.)
└── tests/                # Test directory
    ├── conftest.py       # Pytest fixtures
    ├── test_expr.py      # Unit tests for expr.py
    ├── test_query.py     # Unit tests for query.py
    └── test_sql.py       # Integration tests for SQL generation
```

## Repository Information

- **Git repository**: github.com:rchui/vw.git
- **Default branch**: main
- **Current version**: 0.0.1

## Quick Example

```python
import vw

# Create sources
users = vw.Source(name="users")
orders = vw.Source(name="orders")

# Create parameters
user_id = vw.param("user_id", 123)
status = vw.param("status", "active")

# Build query with joins and parameters
query = users.join.inner(
    orders,
    on=[users.col("id") == user_id, orders.col("status") == status]
)

# Render to SQL and parameters
result = query.select(users.col("name"), orders.col("total")).render()

# result.sql: "SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = :user_id AND orders.status = :status"
# result.params: {"user_id": 123, "status": "active"}
```

## Documentation Guide

- **[setup.md](setup.md)** - How to install and configure the project
- **[api.md](api.md)** - Complete API reference with examples
- **[architecture.md](architecture.md)** - Design decisions and technical details
- **[testing.md](testing.md)** - Testing conventions and how to run tests
- **[features.md](features.md)** - Current features and future roadmap
