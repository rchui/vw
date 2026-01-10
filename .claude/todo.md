# Technical Debt / Future Work

## Rendering Architecture

### Separate Dialect Rendering from Parameter Placeholder Rendering
Currently, dialect-specific rendering (e.g., PostgreSQL vs SQL Server syntax) and parameter placeholder rendering are intertwined in the same `__vw_render__` methods. This should be separated:

1. **Dialect rendering** handles syntax differences (e.g., `DATEADD()` vs `DATE_ADD()` vs `+ INTERVAL`)
2. **Parameter placeholder rendering** handles parameter style (`:name`, `$name`, `@name`)

The `RenderContext` and `RenderConfig` should handle parameter placeholders at a single point, while dialect-specific classes handle SQL syntax. This will make it easier to:
- Add new dialects without duplicating parameter logic
- Support mixed-dialect scenarios
- Improve testability of rendering logic
