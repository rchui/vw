# vw

**SQL, made composable.**

vw is a lightweight library that brings Polars-inspired method chaining to the world of SQL. It’s built for developers who love the expressiveness of SQL but are tired of fighting the fragility of string concatenation and the "black box" of heavy ORMs.

Stop fearing the complexity of large queries. vw lets you fully embrace SQL by treating query segments as first-class, reusable building blocks. Build, pipe, and compose your logic with the fluidity of a modern fluent API, while keeping the full power of the database exactly where it belongs—at your fingertips.

- Love SQL: No abstractions that hide the engine. Write real SQL logic, just more cleanly.

- Lose the Fear: Break "monster queries" into small, testable, and chainable methods.

- Polars-Inspired: If you can use a data frame, you can build a query.

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


## Personal Note

This project was kickstarted by this article from HackerNews:

- https://willmcgugan.github.io/toad-released/

I will readily admit that this repo and the contents within with heavy assistance from LLMs. I've had the idea for this library for a few years and I've never had the time, nor the energy, to put pen to paper. Without these tools, it is unlikely that it would have ever seen the light of day. And for that I am thankful.
