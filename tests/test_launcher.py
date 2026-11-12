# mypy: ignore-errors
import importlib

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from microagent.launcher import (
    GroupInterrupt,
    _run_agent,  # noqa: PLC2701
    init_agent,
    load_configuration,
)


def test_load_configuration_ok(monkeypatch):
    cfg = MagicMock(
        BUS={
            'default': {
                'backend': 'microagent.tools.redis.RedisSignalBus',
                'dsn': 'redis://localhost',
                'prefix': 'APP',
            },
            'failed': {
                'dsn': 'redis://localhost',
            },
        },
        BROKER={
            'main': {
                'backend': 'microagent.tools.amqp.AMQPBroker',
                'dsn': 'amqp://guest:guest@localhost:5671/myhost',
            },
            'bro': {
                'backend': 'microagent.tools.redis.RedisSignalBus',
                'dsn': 'redis://localhost',
            },
            'failed': {
                'dsn': 'redis://localhost',
            },
        },
        AGENT={
            'agent1': {
                'backend': 'app.agent.AppAgent1',
                'bus': 'default',
                'broker': 'main',
            },
            'agent2': {
                'backend': 'app.agent.AppAgent2',
                'broker': 'bro',
            },
            'agent3': {
                'backend': 'app.agent.AppAgent3',
                'bus': 'default',
            },
            'agent4': {
                'backend': 'app.agent.AppAgent4',
            },
            'failed': {
                'bus': 'failed',
            }
        }
    )

    _BUS = (
        'microagent.tools.redis.RedisSignalBus',
        {'dsn': 'redis://localhost', 'prefix': 'APP'}
    )

    _BROKER1 = (
        'microagent.tools.amqp.AMQPBroker',
        {'dsn': 'amqp://guest:guest@localhost:5671/myhost'}
    )

    _BROKER2 = (
        'microagent.tools.redis.RedisSignalBus',
        {'dsn': 'redis://localhost'}
    )

    monkeypatch.setattr(importlib, 'import_module', lambda x: cfg)

    data = list(load_configuration('path'))

    assert len(data) == 4
    assert data[0] == ('agent1', ('app.agent.AppAgent1', {'bus': _BUS, 'broker': _BROKER1}))
    assert data[1] == ('agent2', ('app.agent.AppAgent2', {'bus': None, 'broker': _BROKER2}))
    assert data[2] == ('agent3', ('app.agent.AppAgent3', {'bus': _BUS, 'broker': None}))
    assert data[3] == ('agent4', ('app.agent.AppAgent4', {'bus': None, 'broker': None}))


def test_init_agent_ok(monkeypatch):
    _BUS = (
        'microagent.tools.redis.RedisSignalBus',
        {'dsn': 'redis://localhost', 'prefix': 'APP'}
    )

    _BROKER = (
        'microagent.tools.amqp.AMQPBroker',
        {'dsn': 'amqp://guest:guest@localhost:5671/myhost'}
    )

    agent = MagicMock()
    AppAgent = MagicMock(return_value=agent)
    mod = MagicMock(AppAgent=AppAgent)
    monkeypatch.setattr(importlib, 'import_module', Mock(return_value=mod))

    assert init_agent('app.mod.AppAgent', {'bus': _BUS, 'broker': _BROKER}) is agent
    AppAgent.assert_called_once()

    assert init_agent('app.mod.AppAgent', {}) is agent


async def test_run_agent_exit_code_on_error(monkeypatch):
    # Ошибка агента не должна глушиться SystemExit(0) — процесс обязан упасть с кодом 1
    agent = MagicMock()
    agent.start = AsyncMock(side_effect=RuntimeError('boom'))
    agent.stop = AsyncMock()
    monkeypatch.setattr(
        'microagent.launcher.init_agent', Mock(return_value=agent))

    with pytest.raises(SystemExit) as exc_info:
        await _run_agent('agent1', 'app.mod.AppAgent', {})

    assert exc_info.value.code == 1
    agent.stop.assert_awaited_once()


async def test_run_agent_exit_code_on_interrupt(monkeypatch):
    # Штатное прерывание (сигнал мастера) — выход с кодом 0
    agent = MagicMock()
    agent.start = AsyncMock(side_effect=GroupInterrupt('TERM'))
    agent.stop = AsyncMock()
    monkeypatch.setattr(
        'microagent.launcher.init_agent', Mock(return_value=agent))

    with pytest.raises(SystemExit) as exc_info:
        await _run_agent('agent1', 'app.mod.AppAgent', {})

    assert exc_info.value.code == 0
    agent.stop.assert_awaited_once()
