import pytest

import vw.reference as vw


@pytest.fixture
def render_config() -> vw.RenderConfig:
    return vw.RenderConfig()


@pytest.fixture
def render_context(render_config: vw.RenderConfig) -> vw.RenderContext:
    return vw.RenderContext(config=render_config)
