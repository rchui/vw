"""Tests for vw.duckdb.base module."""

from vw.core.base import Factories
from vw.core.states import Column, Reference
from vw.duckdb.base import Expression, RowSet


def test_expression_creation() -> None:
    """Test that Expression can be created with proper state."""
    factories = Factories(expr=Expression, rowset=RowSet)
    state = Column(name="test")
    expr = Expression(state=state, factories=factories)

    assert isinstance(expr, Expression)
    assert isinstance(expr.state, Column)
    assert expr.factories == factories


def test_rowset_creation() -> None:
    """Test that RowSet can be created with proper state."""
    factories = Factories(expr=Expression, rowset=RowSet)
    state = Reference(name="test")
    rowset = RowSet(state=state, factories=factories)

    assert isinstance(rowset, RowSet)
    assert isinstance(rowset.state, Reference)
    assert rowset.factories == factories
