# Technical Debt / Future Work

## ty type checker workaround

**Added:** 2026-01-09
**Issue:** The `.str` property on `Expression` class shadows the builtin `str` type, causing ty to report false positive `invalid-type-form` errors for any method in the class that uses `str` as a type annotation.

**Workaround:** Added `ignore = ["invalid-type-form"]` to `[tool.ty.rules]` in pyproject.toml.

**Action:** When ty fixes this issue (property names shadowing builtin types in class scope), remove the ignore rule from pyproject.toml and verify ty passes.

**Tracking:** Check ty releases at https://github.com/astral-sh/ty for fixes related to property name shadowing.
