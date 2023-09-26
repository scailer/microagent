# mypy: ignore-errors
from unittest.mock import AsyncMock, Mock

import pytest

from microagent.hooks import Hook, Hooks


@pytest.fixture
def hooks():
    return (
        Hook(agent=None, handler=AsyncMock(), label='pre_start'),
        Hook(agent=None, handler=Mock(), label='pre_start'),
        Hook(agent=None, handler=AsyncMock(), label='post_start'),
        Hook(agent=None, handler=Mock(), label='post_start'),
        Hook(agent=None, handler=AsyncMock(), label='pre_stop'),
        Hook(agent=None, handler=Mock(), label='pre_stop'),
        Hook(agent=None, handler=AsyncMock(), label='server'),
    )


async def test_pre_start_ok(hooks):
    await Hooks(hooks).pre_start()
    hooks[0].handler.assert_called_once()
    hooks[1].handler.assert_called_once()
    hooks[4].handler.assert_not_called()


async def test_post_start_ok(hooks):
    await Hooks(hooks).post_start()
    hooks[2].handler.assert_called_once()
    hooks[3].handler.assert_called_once()
    hooks[4].handler.assert_not_called()


async def test_pre_stop_ok(hooks):
    await Hooks(hooks).pre_stop()
    hooks[4].handler.assert_called_once()
    hooks[5].handler.assert_called_once()
    hooks[0].handler.assert_not_called()


async def test_server_ok(hooks):
    assert list(Hooks(hooks).servers)[0] is hooks[-1].handler
