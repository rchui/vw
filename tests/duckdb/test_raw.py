"""Tests for DuckDB raw SQL API."""

from vw.core.states import RawExpr, RawSource
from vw.duckdb import raw
from vw.duckdb.base import Expression, RowSet
from vw.duckdb.public import col, param
from vw.duckdb.states import Column


def describe_raw_import():
    """Test that raw is importable from vw.duckdb."""

    def it_is_importable_from_package():
        from vw.duckdb import raw as raw_import

        assert raw_import is raw


def describe_raw_expr():
    """Tests for raw.expr() factory."""

    def it_creates_raw_expr():
        expr = raw.expr("some_func()")
        assert isinstance(expr, Expression)
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "some_func()"
        assert expr.state.params == ()

    def it_creates_raw_expr_with_dependencies():
        expr = raw.expr("{a} @> {b}", a=col("tags"), b=param("tag", "python"))
        assert isinstance(expr.state, RawExpr)
        assert expr.state.sql == "{a} @> {b}"
        assert len(expr.state.params) == 2

    def it_creates_raw_expr_with_column_param():
        expr = raw.expr("UPPER({name})", name=col("username"))
        assert isinstance(expr.state, RawExpr)
        assert expr.state.params[0] == ("name", Column(name="username"))


def describe_raw_rowset():
    """Tests for raw.rowset() factory."""

    def it_creates_raw_rowset():
        rowset = raw.rowset("generate_series(1, 10) AS t(n)")
        assert isinstance(rowset, RowSet)
        assert isinstance(rowset.state, RawSource)
        assert rowset.state.sql == "generate_series(1, 10) AS t(n)"

    def it_creates_raw_rowset_with_dependencies():
        rowset = raw.rowset("generate_series(1, {n}) AS t(num)", n=param("max", 10))
        assert isinstance(rowset.state, RawSource)
        assert rowset.state.sql == "generate_series(1, {n}) AS t(num)"
        assert len(rowset.state.params) == 1


def describe_raw_func():
    """Tests for raw.func() factory."""

    def it_creates_raw_func():
        from vw.core.states import Function

        expr = raw.func("MY_FUNC", col("x"), col("y"))
        assert isinstance(expr, Expression)
        assert isinstance(expr.state, Function)
        assert expr.state.name == "MY_FUNC"
        assert len(expr.state.args) == 2
