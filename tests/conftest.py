import pytest

from microagent import Queue, Signal


@pytest.fixture(autouse=True)
async def flush_signals_and_queues() -> None:
    Queue._queues = {}
    Signal._signals = {}
