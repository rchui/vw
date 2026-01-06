# Testing

## Test Structure

- **tests/test_expr.py** - Unit tests for expression classes (Column, Parameter, comparison operators)
- **tests/test_query.py** - Unit tests for query builder (Source, Statement, joins)
- **tests/test_sql.py** - Integration tests for complete SQL generation
- **tests/conftest.py** - Pytest fixtures (render_config, render_context)

## Test Style

### pytest-describe
Uses BDD-style organization for better readability:

```python
def describe_parameter():
    """Tests for Parameter class."""

    def it_renders_parameter_with_colon_style():
        """Should render parameter with colon prefix."""
        # test code
```

### Test Naming
- `describe_*` for test groups
- `it_*` for individual tests
- Descriptive names that read like sentences

### Test Focus
- Test behavior, not implementation details
- Don't test dataclass field assignments (they work by default)
- Focus on `__vw_render__()` output and parameter collection

## Running Tests

```bash
# Run all tests
uv run pytest -v

# Run specific test file
uv run pytest tests/test_expr.py -v

# Stop on first failure, verbose, no capture
uv run pytest -xvs

# Run tests matching a pattern
uv run pytest -k "parameter" -v
```

## Writing Tests

### Unit Tests

Test individual components in isolation:

```python
def it_renders_parameter_with_colon_style(render_context: vw.RenderContext) -> None:
    """Should render parameter with colon prefix and register in context."""
    param = vw.param("age", 25)
    assert param.__vw_render__(render_context) == ":age"
    assert render_context.params == {"age": 25}
```

### Integration Tests

Test complete SQL generation:

```python
def it_generates_select_with_parameter_in_join(render_config: vw.RenderConfig) -> None:
    """Should generate query with parameter in join condition."""
    users = vw.Source(name="users")
    orders = vw.Source(name="orders")
    status_param = vw.param("status", "active")

    joined = users.join.inner(
        orders,
        on=[users.col("id") == orders.col("user_id"), users.col("status") == status_param]
    )

    result = joined.select(vw.col("*")).render(config=render_config)

    assert result == vw.RenderResult(
        sql="SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id AND users.status = :status",
        params={"status": "active"}
    )
```

### Using Fixtures

Two main fixtures in `conftest.py`:

```python
@pytest.fixture
def render_config() -> vw.RenderConfig:
    return vw.RenderConfig()

@pytest.fixture
def render_context(render_config: vw.RenderConfig) -> vw.RenderContext:
    return vw.RenderContext(config=render_config)
```

Use `render_context` for testing `__vw_render__()` directly.
Use `render_config` for testing `Statement.render()`.

## Linting

All tests must pass ruff checks:

```bash
uv run ruff check .
uv run ruff format .
```
