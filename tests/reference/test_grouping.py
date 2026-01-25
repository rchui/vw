"""Unit tests for grouping constructs (ROLLUP, CUBE, GROUPING SETS)."""

import vw.reference as vw
from vw.reference.grouping import Cube, GroupingSets, Rollup


def describe_rollup():
    """Tests for ROLLUP expression."""

    def it_renders_single_column(render_context: vw.RenderContext) -> None:
        result = Rollup(columns=[vw.col("year")]).__vw_render__(render_context)
        assert result == "ROLLUP (year)"

    def it_renders_multiple_columns(render_context: vw.RenderContext) -> None:
        result = Rollup(columns=[vw.col("year"), vw.col("quarter"), vw.col("month")]).__vw_render__(render_context)
        assert result == "ROLLUP (year, quarter, month)"

    def it_renders_empty_columns(render_context: vw.RenderContext) -> None:
        result = Rollup(columns=[]).__vw_render__(render_context)
        assert result == "ROLLUP ()"


def describe_cube():
    """Tests for CUBE expression."""

    def it_renders_single_column(render_context: vw.RenderContext) -> None:
        result = Cube(columns=[vw.col("region")]).__vw_render__(render_context)
        assert result == "CUBE (region)"

    def it_renders_multiple_columns(render_context: vw.RenderContext) -> None:
        result = Cube(columns=[vw.col("region"), vw.col("product")]).__vw_render__(render_context)
        assert result == "CUBE (region, product)"

    def it_renders_empty_columns(render_context: vw.RenderContext) -> None:
        result = Cube(columns=[]).__vw_render__(render_context)
        assert result == "CUBE ()"


def describe_grouping_sets():
    """Tests for GROUPING SETS expression."""

    def it_renders_single_set(render_context: vw.RenderContext) -> None:
        result = GroupingSets(sets=[(vw.col("year"), vw.col("region"))]).__vw_render__(render_context)
        assert result == "GROUPING SETS ((year, region))"

    def it_renders_multiple_sets(render_context: vw.RenderContext) -> None:
        result = GroupingSets(
            sets=[
                (vw.col("year"), vw.col("region")),
                (vw.col("year"),),
            ]
        ).__vw_render__(render_context)
        assert result == "GROUPING SETS ((year, region), (year))"

    def it_renders_empty_set_for_grand_total(render_context: vw.RenderContext) -> None:
        result = GroupingSets(
            sets=[
                (vw.col("year"),),
                (),
            ]
        ).__vw_render__(render_context)
        assert result == "GROUPING SETS ((year), ())"

    def it_renders_only_grand_total(render_context: vw.RenderContext) -> None:
        result = GroupingSets(sets=[()]).__vw_render__(render_context)
        assert result == "GROUPING SETS (())"


def describe_helper_functions():
    """Tests for helper functions."""

    def it_creates_rollup_via_helper(render_context: vw.RenderContext) -> None:
        result = vw.rollup(vw.col("year"), vw.col("quarter")).__vw_render__(render_context)
        assert result == "ROLLUP (year, quarter)"

    def it_creates_cube_via_helper(render_context: vw.RenderContext) -> None:
        result = vw.cube(vw.col("region"), vw.col("product")).__vw_render__(render_context)
        assert result == "CUBE (region, product)"

    def it_creates_grouping_sets_via_helper(render_context: vw.RenderContext) -> None:
        result = vw.grouping_sets(
            (vw.col("year"), vw.col("region")),
            (vw.col("year"),),
            (),
        ).__vw_render__(render_context)
        assert result == "GROUPING SETS ((year, region), (year), ())"
