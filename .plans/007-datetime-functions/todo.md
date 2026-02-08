# Todo: Phase 7 Date/Time Functions

## Tasks

- [ ] 1. Add `Extract`, `CurrentTimestamp`, `CurrentDate`, `CurrentTime` states to `vw/core/states.py`
- [ ] 2. Create `vw/core/datetime.py` with `DateTimeAccessor` class (`.extract(field: str)`)
- [ ] 3. Add `.dt` property to `Expression` in `vw/core/base.py`
- [ ] 4. Add `current_timestamp()`, `current_date()`, `current_time()` to `Functions` in `vw/core/functions.py`
- [ ] 5. Add `Now` and `Interval` states to `vw/postgres/` (in `render.py` or new states module)
- [ ] 6. Create `vw/postgres/datetime.py` with `PostgresDateTimeAccessor(DateTimeAccessor)`
- [ ] 7. Override `.dt` property on postgres `Expression` in `vw/postgres/base.py`
- [ ] 8. Add `now()` to postgres `Functions` in `vw/postgres/public.py`
- [ ] 9. Add `interval()` top-level factory to `vw/postgres/public.py`
- [ ] 10. Export `interval` from `vw/postgres/__init__.py`
- [ ] 11. Add rendering cases for all new states in `vw/postgres/render.py`
- [ ] 12. Create `tests/postgres/integration/test_datetime_functions.py` with full test suite
- [ ] 13. Update `docs/development/postgres-parity.md` to mark completed items
