"""Tests for vw/base.py module."""

import vw.reference as vw
from vw.reference.base import RowSet
from vw.reference.column import Column


def describe_rowset() -> None:
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

    def describe_star() -> None:
        """Tests for RowSet.star() method."""

        def it_creates_star_expression() -> None:
            """star() should return a StarExpression."""
            source = vw.Source(name="users")
            star_expr = source.star
            assert isinstance(star_expr, vw.StarExpression)

        def it_renders_star_expression(render_context: vw.RenderContext) -> None:
            """StarExpression should render correctly."""
            source = vw.Source(name="users")
            star_expr = source.star
            assert star_expr.__vw_render__(render_context) == "*"

        def it_renders_aliased_star_expression(render_context: vw.RenderContext) -> None:
            """StarExpression from aliased RowSet should render correctly."""
            aliased = vw.Source(name="users").alias("u")
            star_expr = aliased.star
            assert star_expr.__vw_render__(render_context) == "u.*"
