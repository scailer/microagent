import pytest

import microagent.bus as bus_module

from microagent import Queue, Signal


@pytest.fixture(autouse=True)
async def flush_signals_and_queues() -> None:
    Queue._queues = {}
    Signal._signals = {}
    bus_module._DEFAULT_PREFIX = 'PUBSUB'
