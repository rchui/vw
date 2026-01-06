"""Tests for vw/base.py module."""

import vw
from vw.base import RowSet
from vw.column import Column


def describe_alias() -> None:
    """Tests for RowSet.alias() method."""

    def it_renders_source_with_alias(render_context: vw.RenderContext) -> None:
        """Should render source with AS alias."""
        source = vw.Source(name="users")
        aliased = source.alias("u")
        assert isinstance(aliased, vw.Source)
        assert aliased.__vw_render__(render_context) == "users AS u"

    def it_creates_qualified_column_from_alias(render_context: vw.RenderContext) -> None:
        """Should create column qualified with alias name."""
        aliased = vw.Source(name="users").alias("u")
        column = aliased.col("id")
        assert isinstance(column, Column)
        assert column.__vw_render__(render_context) == "u.id"

    def it_returns_same_type(render_context: vw.RenderContext) -> None:
        """alias() should return the same type as the original."""
        source = vw.Source(name="users")
        aliased = source.alias("u")
        assert type(aliased) is vw.Source

    def it_is_a_rowset() -> None:
        """Aliased source should still be a RowSet."""
        aliased = vw.Source(name="users").alias("u")
        assert isinstance(aliased, RowSet)
