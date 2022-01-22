# mypy: ignore-errors
import asyncio
import inspect

import pytest
from microagent import Signal, Queue


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def pytest_collection_modifyitems(session, config, items):
    for item in items:
        if isinstance(item, pytest.Function) and inspect.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)


@pytest.fixture(autouse=True)
async def flush_signals_and_queues():
    Queue._queues = {}
    Signal._signals = {}
