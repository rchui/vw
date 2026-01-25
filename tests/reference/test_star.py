"""Tests for vw/star.py module."""

import vw.reference as vw


def describe_star_expression() -> None:
    def it_renders_star(render_context: vw.RenderContext) -> None:
        """Should render basic star expression."""
        star_expr = vw.star
        assert star_expr.__vw_render__(render_context) == "*"

    def it_renders_multiple_clauses(render_context: vw.RenderContext) -> None:
        """Should render star expression with multiple clauses."""
        star_expr = (
            vw.star.exclude(vw.col("password"))
            .replace(vw.col("old_name").alias("new_name"))
            .rename(vw.col("temp").alias("permanent"))
        )
        assert (
            star_expr.__vw_render__(render_context)
            == "* EXCLUDE (password) REPLACE (old_name AS new_name) RENAME (temp AS permanent)"
        )

    def describe_exclude() -> None:
        """Tests for StarExpression.exclude() method."""

        def it_adds_exclude_clause(render_context: vw.RenderContext) -> None:
            """Should render star expression with EXCLUDE clause."""
            star_expr = vw.star.exclude(vw.col("password"), vw.col("ssn"))
            assert star_expr.__vw_render__(render_context) == "* EXCLUDE (password, ssn)"

    def describe_replace() -> None:
        """Tests for StarExpression.replace() method."""

        def it_adds_replace_clause(render_context: vw.RenderContext) -> None:
            """Should render star expression with REPLACE clause."""
            star_expr = vw.star.replace(vw.col("old_name").alias("new_name"))
            assert star_expr.__vw_render__(render_context) == "* REPLACE (old_name AS new_name)"

    def describe_rename() -> None:
        """Tests for StarExpression.rename() method."""

        def it_adds_rename_clause(render_context: vw.RenderContext) -> None:
            """Should render star expression with RENAME clause."""
            star_expr = vw.star.rename(vw.col("old_name").alias("new_name"))
            assert star_expr.__vw_render__(render_context) == "* RENAME (old_name AS new_name)"

    def describe_like() -> None:
        """Tests for StarExpression.like() method."""

        def it_adds_like_clause(render_context: vw.RenderContext) -> None:
            """Should render star expression with LIKE clause."""
            star_expr = vw.star.like("user_%")
            assert star_expr.__vw_render__(render_context) == "* LIKE 'user_%'"
