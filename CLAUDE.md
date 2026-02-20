---
name: 'LLM AI coding agent'
description: 'Optimize for model reasoning, regeneration, and debugging.'
---

You are an AI-first software engineer. Assume all code will be written and maintained by LLMs, not humans. Optimize for model reasoning, regeneration, and debugging — not human aesthetics.

Your goal: produce code that is predictable, debuggable, and easy for future LLMs to rewrite or extend.

ALWAYS use #runSubagent. Your context window size is limited - especially the output. So you should always work in discrete steps and run each step using #runSubAgent. You want to avoid putting anything in the main context window when possible.

ALWAYS use #context7 MCP Server to read relevant documentation. Do this every time you are working with a language, framework, library etc. Never assume that you know the answer as these things change frequently. Your training date is in the past so your knowledge is likely out of date, even if it is a technology you are familiar with.

ALWAYS check your work before returning control to the user. Run tests if available, verify builds, etc. Never return incomplete or unverified work to the user.

Be a good steward of terminal instances. Try and reuse existing terminals where possible and use the VS Code API to close terminals that are no longer needed each time you open a new terminal.

## Mandatory Coding Principles

These coding principles are mandatory:

1. Structure
- Use a consistent, predictable project layout.
- Group code by feature/screen; keep shared utilities minimal.
- Create simple, obvious entry points.
- Before scaffolding multiple files, identify shared structure first. Use framework-native composition patterns (layouts, base templates, providers, shared components) for elements that appear across pages. Duplication that requires the same fix in multiple places is a code smell, not a pattern to preserve.

2. Architecture
- Prefer flat, explicit code over abstractions or deep hierarchies.
- Avoid clever patterns, metaprogramming, and unnecessary indirection.
- Minimize coupling so files can be safely regenerated.

3. Functions and Modules
- Keep control flow linear and simple.
- Use small-to-medium functions; avoid deeply nested logic.
- Pass state explicitly; avoid globals.

4. Naming and Comments
- Use descriptive-but-simple names.
- Comment only to note invariants, assumptions, or external requirements.

5. Logging and Errors
- Emit detailed, structured logs at key boundaries.
- Make errors explicit and informative.

6. Regenerability
- Write code so any file/module can be rewritten from scratch without breaking the system.
- Prefer clear, declarative configuration (JSON/YAML/etc.).

7. Platform Use
- Use platform conventions directly and simply (e.g., WinUI/WPF) without over-abstracting.

8. Modifications
- When extending/refactoring, follow existing patterns.
- Prefer full-file rewrites over micro-edits unless told otherwise.

9. Quality
- Favor deterministic, testable behavior.
- Keep tests simple and focused on verifying observable behavior.

10. Incrementality
- Before starting any work, breakdown the work that needs to be done into a list of sequential, atomic, complete changes.
- Favor working in digestable pieces that can be easily rolled back.
- Keep PRs as simple and focused on the task at hand.
- Leave breadcrumb comments where known future work will go.

11. Testability
- Work should be continuously tested at the end of each piece of work.
- New work should add unit tests (mandatory), and integration tests (if appropriate).
- Ensure that all tests, linters, and type checks are successful before moving on to the next piece of work.
- When asserting containers or class instances the test should assert the entire container or instance if possible instead of asserting the parts.

```bash
uv run pytest          # Run tests
uv run ruff check      # Lint
uv run ruff format     # Format
uv run ty check        # Type check
```

12. Refactoring
- When making a change, consider how that change fits in with the rest of the codebase.
- Look for opportunities to refactor related code when making any changes.
- Remove or simplify code whenever possible.

13. Python
- Favor `from <module> import <ref> as <name>` over `__all__`.
- When importing modules, use the following hierarchy
  1. TYPE_CHECKING imports if the import is only used for type checking
  2. module level imports
  3. inline function level imports if there is a circular dependency

## Documentation Index

**IMPORTANT: This index must be kept up to date whenever code is added, removed, or moved. Each time you complete a task or learn important information about the project, you should update the docs. Reflect any new information that you've learned or changes that require updates to these documentation files.**

### Core Concepts & Getting Started
- What is vw? → `docs/index.md` (Overview), `docs/getting-started.md` (What is vw?)
- Why use vw? → `docs/index.md` (Why vw?), `docs/getting-started.md` (Why Use vw?)
- Immutability pattern → `docs/getting-started.md` (Core Concepts > Immutable State)
- Method chaining → `docs/getting-started.md` (Core Concepts > Method Chaining)
- Parameters and SQL injection prevention → `docs/getting-started.md` (Core Concepts > Parameters)
- Basic workflow → `docs/getting-started.md` (Basic Workflow)

### Installation & Setup
- Install with uv/pip → `docs/installation.md` (Install with uv, Install with pip)
- Development installation → `docs/installation.md` (Development Installation)
- Database drivers (PostgreSQL, DuckDB) → `docs/installation.md` (Database Drivers)
- Using with SQLAlchemy → `docs/installation.md` (Example with SQLAlchemy), `docs/api/postgres.md` (Using with SQLAlchemy)
- Using with psycopg2 → `docs/installation.md` (Example with psycopg2)
- Using with asyncpg → `docs/api/postgres.md` (Using with asyncpg)
- Using with DuckDB → `docs/installation.md` (Example with DuckDB)

### Architecture & Design
- Component overview → `docs/architecture.md` (Component Overview)
- vw/core purpose → `docs/architecture.md` (vw/core - Abstract Core)
- vw/postgres purpose → `docs/architecture.md` (vw/postgres - PostgreSQL Dialect)
- vw/duckdb purpose → `docs/architecture.md` (vw/duckdb - DuckDB Dialect)
- State pattern → `docs/architecture.md` (vw/core - Abstract Core > States)
- Wrapper pattern → `docs/architecture.md` (vw/core - Abstract Core > Wrappers)
- Design patterns → `docs/architecture.md` (Design Patterns)
- Query building flow → `docs/architecture.md` (Query Building Flow)
- Adding new dialect → `docs/architecture.md` (Extension Points > Adding a New Dialect)
- Adding new feature → `docs/architecture.md` (Extension Points > Adding a New Feature)

### Query Building Examples
- Basic SELECT → `docs/quickstart.md` (Basic SELECT)
- SELECT with WHERE → `docs/quickstart.md` (WHERE Conditions)
- Complex WHERE conditions → `docs/quickstart.md` (Complex WHERE)
- Pattern matching (LIKE, ILIKE, IN, BETWEEN) → `docs/quickstart.md` (Pattern Matching), `docs/api/postgres.md` (Pattern Matching)
- Arithmetic operations → `docs/quickstart.md` (Arithmetic)
- Aggregation (COUNT, SUM, AVG) → `docs/quickstart.md` (Aggregation)
- GROUP BY and HAVING → `docs/quickstart.md` (Aggregation)
- Window functions → `docs/quickstart.md` (Window Functions)
- Sorting (ORDER BY) → `docs/quickstart.md` (Sorting and Limiting)
- Pagination (LIMIT, OFFSET, FETCH) → `docs/quickstart.md` (Sorting and Limiting), `docs/api/postgres.md` (Query Modifiers)
- CASE/WHEN expressions → `docs/quickstart.md` (Conditional Expressions), `docs/api/postgres.md` (Conditional Expressions), `docs/api/core.md` (Conditional Expressions)
- Subqueries → `docs/quickstart.md` (Subqueries)
- VALUES clause → `docs/quickstart.md` (VALUES Clause), `docs/api/postgres.md` (Factory Functions > values)
- Set operations (UNION, INTERSECT, EXCEPT) → `docs/quickstart.md` (Set Operations), `docs/api/postgres.md` (Set Operations)
- Joins (INNER, LEFT, RIGHT, FULL, CROSS) → `docs/quickstart.md` (Joins)
- LATERAL joins → `docs/api/postgres.md` (LATERAL Joins)
- CTEs → `docs/quickstart.md` (CTEs)
- FILTER clause → `docs/quickstart.md` (FILTER Clause)
- Grouping Sets (ROLLUP, CUBE, GROUPING SETS) → `docs/quickstart.md` (Grouping Sets), `docs/api/postgres.md` (Factory Functions > rollup/cube/grouping_sets)
- Query modifiers (FOR UPDATE, TABLESAMPLE) → `docs/api/postgres.md` (Query Modifiers)
- Row-level locking (modifiers.row_lock) → `docs/api/postgres.md` (Row-Level Locking)
- SQLAlchemy integration → `docs/quickstart.md` (Using with SQLAlchemy), `docs/api/postgres.md` (Using with SQLAlchemy)

### API Reference - Expression Class
- Generic infix operator (.op()) → `docs/api/core.md` (Generic Operator)
- Comparison operators (==, !=, <, <=, >, >=) → `docs/api/core.md` (Comparison Operators)
- Arithmetic operators (+, -, *, /, %) → `docs/api/core.md` (Arithmetic Operators)
- Logical operators (&, |, ~) → `docs/api/core.md` (Logical Operators)
- .like() and .not_like() → `docs/api/core.md` (Pattern Matching)
- .ilike() and .not_ilike() (PostgreSQL case-insensitive) → `docs/api/postgres.md` (Pattern Matching)
- .is_in() and .is_not_in() → `docs/api/core.md` (Pattern Matching)
- .between() and .not_between() → `docs/api/core.md` (Pattern Matching)
- .is_null() and .is_not_null() → `docs/api/core.md` (NULL Checks)
- .alias(), .cast(), .asc(), .desc() → `docs/api/core.md` (Expression Modifiers)
- .dt.extract(field) (EXTRACT) → `docs/api/core.md` (Date/Time Accessor)
- when().then().otherwise() / .end() (CASE WHEN) → `docs/api/core.md` (Conditional Expressions)
- .over() (window specification) → `docs/api/core.md` (Window Function Methods)
- .filter() (FILTER clause) → `docs/api/core.md` (Window Function Methods)
- .rows_between(), .range_between(), .exclude() → `docs/api/core.md` (Window Function Methods)

### API Reference - RowSet Class
- .select(), .where(), .group_by(), .having(), .order_by(), .limit(), .distinct() → `docs/api/core.md` (RowSet)
- .offset() (separate from .limit()) → `docs/api/postgres.md` (Query Modifiers > .offset)
- .fetch() (FETCH FIRST with WITH TIES support) → `docs/api/postgres.md` (Query Modifiers > .fetch)
- .modifiers() (row-level locking, table sampling) → `docs/api/postgres.md` (Query Modifiers > .modifiers)
- modifiers.row_lock() (typed FOR UPDATE/SHARE) → `docs/api/postgres.md` (Row-Level Locking)
- .alias(), .col(), .star → `docs/api/core.md` (RowSet)
- .join.inner/left/right/full_outer/cross → `docs/api/core.md` (RowSet > .join)
- .join with lateral=True (PostgreSQL LATERAL) → `docs/api/postgres.md` (LATERAL Joins)

### API Reference - Functions
- F.count/sum/avg/min/max (ANSI aggregates) → `docs/api/core.md` (Functions > Aggregate Functions)
- F.bool_and/bool_or (boolean aggregates) → `docs/api/postgres.md` (Boolean and Bitwise Aggregate Functions)
- F.bit_and/bit_or (bitwise aggregates) → `docs/api/postgres.md` (Boolean and Bitwise Aggregate Functions)
- F.row_number/rank/dense_rank/ntile/lag/lead/first_value/last_value (window) → `docs/api/core.md` (Functions > Window Functions)
- F.current_timestamp/current_date/current_time (ANSI date/time) → `docs/api/core.md` (Functions > Date/Time Functions)
- F.grouping() (grouping sets) → `docs/api/core.md` (Functions > Grouping Functions), `docs/api/postgres.md` (Date/Time > Grouping Functions)
- F.now() (PostgreSQL) → `docs/api/postgres.md` (Date/Time)
- F.gen_random_uuid() (PostgreSQL UUID) → `docs/api/postgres.md` (PostgreSQL Convenience Functions > UUID Functions)
- F.array_agg() (with order_by support) → `docs/api/postgres.md` (PostgreSQL Convenience Functions > Array Aggregate Functions)
- F.string_agg() (with order_by support) → `docs/api/postgres.md` (PostgreSQL Convenience Functions > Array Aggregate Functions)
- F.json_build_object() → `docs/api/postgres.md` (PostgreSQL Convenience Functions > JSON Functions)
- F.json_agg() (with order_by support) → `docs/api/postgres.md` (PostgreSQL Convenience Functions > JSON Functions)
- F.unnest() → `docs/api/postgres.md` (PostgreSQL Convenience Functions > Array Functions)

### API Reference - Window Frames
- UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, preceding(n), following(n) → `docs/api/core.md` (Frame Boundaries)

### API Reference - PostgreSQL
- ref() factory → `docs/api/postgres.md` (Factory Functions > ref)
- col() factory → `docs/api/postgres.md` (Factory Functions > col)
- param() factory → `docs/api/postgres.md` (Factory Functions > param)
- lit() factory (literals with safe rendering) → `docs/api/postgres.md` (Factory Functions > lit)
- when() factory (CASE WHEN) → `docs/api/postgres.md` (Conditional Expressions)
- exists() factory → `docs/api/postgres.md` (Factory Functions > exists)
- values() factory → `docs/api/postgres.md` (Factory Functions > values)
- cte() factory → `docs/api/postgres.md` (Factory Functions > cte)
- rollup() factory → `docs/api/postgres.md` (Factory Functions > rollup)
- cube() factory → `docs/api/postgres.md` (Factory Functions > cube)
- grouping_sets() factory → `docs/api/postgres.md` (Factory Functions > grouping_sets)
- interval() factory → `docs/api/postgres.md` (Factory Functions > interval)
- render() function → `docs/api/postgres.md` (Factory Functions > render)
- raw.func() convenience → `docs/api/postgres.md` (Raw SQL API > raw.func)
- raw.expr() escape hatch → `docs/api/postgres.md` (Raw SQL API > raw.expr)
- raw.rowset() escape hatch → `docs/api/postgres.md` (Raw SQL API > raw.rowset)
- .dt.date_trunc(unit) (PostgreSQL DATE_TRUNC) → `docs/api/postgres.md` (Date/Time)
- Parameter style (dollar-style) → `docs/api/postgres.md` (Parameter Style)
- PostgreSQL examples → `docs/api/postgres.md` (Complete Examples)
- Feature status → `docs/api/postgres.md` (Feature Status)

### API Reference - DuckDB
- Current status → `docs/api/duckdb.md` (Development Status)
- DuckDB raw API (raw.expr/rowset/func) → `docs/api/duckdb.md` (Raw SQL API)
- .sample() (USING SAMPLE clause) → `docs/api/duckdb.md` (Sampling)
- modifiers.using_sample() (typed USING SAMPLE factory) → `docs/api/duckdb.md` (Sampling)
- Planned features → `docs/api/duckdb.md` (Pending DuckDB-Specific Features)

### Development Roadmaps
- PostgreSQL feature roadmap → `docs/development/postgres-parity.md`
- PostgreSQL completed features → `docs/development/postgres-parity.md` (Completed Features)
- DuckDB feature roadmap → `docs/development/duckdb-parity.md`
- DuckDB star extensions (planned) → `docs/development/duckdb-parity.md` (Phase 2: Star Extensions)
- DuckDB file I/O (planned) → `docs/development/duckdb-parity.md` (Phase 3: File I/O)
- Snowflake feature roadmap → `docs/development/snowflake-parity.md`
- MySQL feature roadmap → `docs/development/mysql-parity.md`
- SQL Server feature roadmap → `docs/development/sqlserver-parity.md`
- SQLite feature roadmap → `docs/development/sqlite-parity.md`
- Oracle feature roadmap → `docs/development/oracle-parity.md`
- ClickHouse feature roadmap → `docs/development/clickhouse-parity.md`
