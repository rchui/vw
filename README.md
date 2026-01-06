# vw

A SQL builder library with polars-inspired method chaining syntax for constructing parameterized SQL queries.

## Usage

Here's a simple example using vw with SQLAlchemy:

```python
import vw
from sqlalchemy import create_engine, text

# Create your database connection
engine = create_engine("postgresql://user:password@localhost/mydb")

# Build a query with vw
users = vw.Source(name="users")
orders = vw.Source(name="orders")

user_id = vw.param("user_id", 123)
status = vw.param("status", "active")

query = (
    users
    .join.inner(
        orders,
        on=[
            users.col("id") == user_id,
            orders.col("status") == status
        ]
    )
    .select(users.col("name"), orders.col("total"))
)

# Render to SQL and parameters
result = query.render()

print(result.sql)
# >>> SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = :user_id AND orders.status = :status

print(result.params)
# >>> {"user_id": 123, "status": "active"}

# Execute with SQLAlchemy
with engine.connect() as connection:
    rows = connection.execute(
        text(result.sql),
        result.params
    )
    for row in rows:
        print(row)
```

## Installation

Install using uv:

```bash
uv pip install .
```

For development:

```bash
uv pip install -e ".[dev]"
```

## Development

### Setup

```bash
# Install dependencies
uv pip install -e ".[dev]"
```

### Linting and Formatting

This project uses ruff for linting and formatting:

```bash
# Check code
ruff check .

# Format code
ruff format .

# Fix auto-fixable issues
ruff check --fix .
```

### Running the CLI

```bash
vw --version
vw
```

## License

MIT
