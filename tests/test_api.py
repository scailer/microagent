# mypy: ignore-errors
from pathlib import Path

from microagent import (  # noqa
    MicroAgent,
    Queue,
    Signal,
    __version__,
    consumer,
    cron,
    load_queues,
    load_signals,
    load_stuff,
    periodic,
    receiver,
)
from microagent.tools import mocks


def test_load_from_file():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    signals, queues = load_stuff(source)
    assert len(signals) == 3
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert signals.typed_signal.name == 'typed_signal'
    assert queues.test_queue.name == 'test_queue'


def test_load_signals():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    signals = load_signals(source)
    assert len(signals) == 3
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert signals.typed_signal.name == 'typed_signal'

    assert signals.typed_signal.type_map == {
        'uuid': (str, ),
        'code': (int, type(None)),
        'flag': (bool, ),
        'ids': (list, )
    }


def test_load_queues():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    queues = load_queues(source)
    assert len(queues) == 1
    assert queues.test_queue.name == 'test_queue'


def test_load_from_url():
    source = 'https://raw.githubusercontent.com/scailer/microagent/1.7/tests/stuff.json'
    signals, queues = load_stuff(source)
    assert len(signals) == 3
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert queues.test_queue.name == 'test_queue'


async def test_mock_bus_ok():
    bus = mocks.BusMock()
    await bus.test_signal.send()
    bus.test_signal.send.assert_called()
    await bus.test_signal.call()
    bus.test_signal.call.assert_called()
    assert str(bus)


async def test_mock_broker_ok():
    broker = mocks.BrokerMock()
    await broker.test_queue.send()
    broker.test_queue.send.assert_called()
    await broker.test_queue.length()
    broker.test_queue.length.assert_called()
    await broker.test_queue.declare()
    broker.test_queue.declare.assert_called()
    assert str(broker)
