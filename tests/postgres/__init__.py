"""PostgreSQL-specific tests for vw.postgres module.

This directory contains tests for PostgreSQL-specific extensions and syntax that
are not part of the ANSI SQL standard.

**What belongs here:**
- PostgreSQL-specific extensions or syntax
- Examples: ILIKE, ROLLUP/CUBE/GROUPING SETS, DISTINCT ON, LATERAL,
  DATE_TRUNC, NOW(), ARRAY_AGG, JSON functions, FOR UPDATE, FETCH WITH TIES,
  window frames with EXCLUDE, TABLESAMPLE, etc.

**What belongs in tests/core:**
- ANSI SQL standard features that work identically across all SQL dialects
- Examples: =, <, >, COUNT, SUM, INNER JOIN, CASE/WHEN, CTEs, UNION

Note: Integration tests remain in tests/postgres/integration and test the full
vw.postgres module including both standard SQL and PostgreSQL-specific features.
"""
