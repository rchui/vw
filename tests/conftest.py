import pytest

from vw.core.render import ParamStyle, RenderConfig, RenderContext


@pytest.fixture
def render_config() -> RenderConfig:
    return RenderConfig(param_style=ParamStyle.DOLLAR)


@pytest.fixture
def render_context(render_config: RenderConfig) -> RenderContext:
    return RenderContext(config=render_config)
