# vw

A Python library.

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
