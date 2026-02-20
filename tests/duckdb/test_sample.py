"""Tests for DuckDB RowSet .sample() method."""

import pytest

from vw.duckdb import modifiers, render
from vw.duckdb.public import ref
from vw.duckdb.states import UsingSample


def describe_sample():
    """Tests for DuckDB USING SAMPLE support via .sample() shorthand."""

    def it_samples_by_percent():
        rs = ref("big_table").sample(percent=10)
        assert isinstance(rs.state.modifiers[0], UsingSample)
        assert rs.state.modifiers[0].percent == 10
        assert render(rs).query.endswith("USING SAMPLE 10%")

    def it_samples_by_rows():
        rs = ref("big_table").sample(rows=1000)
        assert isinstance(rs.state.modifiers[0], UsingSample)
        assert rs.state.modifiers[0].rows == 1000
        assert render(rs).query.endswith("USING SAMPLE 1000 ROWS")

    def it_samples_with_method_and_rows():
        sql = render(ref("big_table").sample(method="reservoir", rows=1000))
        assert sql.query.endswith("USING SAMPLE reservoir(1000 ROWS)")

    def it_samples_with_method_bernoulli_and_percent():
        sql = render(ref("big_table").sample(method="bernoulli", percent=10))
        assert sql.query.endswith("USING SAMPLE bernoulli(10%)")

    def it_samples_with_method_system_and_percent():
        sql = render(ref("big_table").sample(method="system", percent=5))
        assert sql.query.endswith("USING SAMPLE system(5%)")

    def it_samples_with_percent_and_seed():
        sql = render(ref("big_table").sample(percent=10, seed=42))
        assert sql.query.endswith("USING SAMPLE 10% REPEATABLE (42)")

    def it_samples_with_method_rows_and_seed():
        sql = render(ref("big_table").sample(method="reservoir", rows=1000, seed=7))
        assert sql.query.endswith("USING SAMPLE reservoir(1000 ROWS) REPEATABLE (7)")

    def it_samples_before_select():
        """Modifier on Reference renders in FROM clause position."""
        sql = render(ref("big_table").sample(percent=10))
        assert "FROM big_table USING SAMPLE 10%" in sql.query

    def it_samples_after_select():
        """Modifier on Statement renders at end."""
        sql = render(ref("big_table").select(ref("big_table").star()).sample(percent=10))
        assert sql.query.endswith("USING SAMPLE 10%")

    def it_raises_when_no_args():
        with pytest.raises(ValueError, match="requires exactly one of"):
            ref("big_table").sample()

    def it_raises_when_both_percent_and_rows():
        with pytest.raises(ValueError, match="only one of"):
            ref("big_table").sample(percent=10, rows=1000)


def describe_using_sample_factory():
    """Tests for modifiers.using_sample() factory."""

    def it_creates_using_sample_expression():
        expr = modifiers.using_sample(percent=10)
        assert isinstance(expr.state, UsingSample)
        assert expr.state.percent == 10

    def it_works_with_modifiers_percent():
        sql = render(ref("big_table").modifiers(modifiers.using_sample(percent=10)))
        assert sql.query.endswith("USING SAMPLE 10%")

    def it_works_with_modifiers_rows():
        sql = render(ref("big_table").modifiers(modifiers.using_sample(rows=1000)))
        assert sql.query.endswith("USING SAMPLE 1000 ROWS")

    def it_works_with_method_and_seed():
        sql = render(ref("big_table").modifiers(modifiers.using_sample(method="reservoir", rows=1000, seed=42)))
        assert sql.query.endswith("USING SAMPLE reservoir(1000 ROWS) REPEATABLE (42)")

    def it_raises_when_no_args():
        with pytest.raises(ValueError):
            modifiers.using_sample()

    def it_raises_when_both_percent_and_rows():
        with pytest.raises(ValueError):
            modifiers.using_sample(percent=10, rows=1000)
