from pathlib import Path
from microagent import (MicroAgent, Signal, Queue, receiver, consumer,  # noqa
    periodic, cron, load_stuff, load_signals, load_queues, __version__)  # noqa
from microagent.tools import mocks


def test_load_from_file():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    signals, queues = load_stuff(source)
    assert len(signals) == 2
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert queues.test_queue.name == 'test_queue'


def test_load_signals():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    signals = load_signals(source)
    assert len(signals) == 2
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'


def test_load_queues():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    queues = load_queues(source)
    assert len(queues) == 1
    assert queues.test_queue.name == 'test_queue'


def test_load_from_url():
    source = 'https://gist.githubusercontent.com/scailer/ee0baed54a9444f328c9d0fd4ed84bed/raw/dfae17935813ee6a0ef0d441cbebc19572f8488c/stuff.json'  # noqa
    signals, queues = load_stuff(source)
    assert len(signals) == 2
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert queues.test_queue.name == 'test_queue'


async def test_mock_bus_ok():
    bus = mocks.BusMock()
    bus.test_signal.send()
    bus.test_signal.send.assert_called()
    bus.test_signal.call()
    bus.test_signal.call.assert_called()
    assert str(bus)


async def test_mock_broker_ok():
    broker = mocks.BrokerMock()
    broker.test_queue.send()
    broker.test_queue.send.assert_called()
    broker.test_queue.length()
    broker.test_queue.length.assert_called()
    broker.test_queue.declare()
    broker.test_queue.declare.assert_called()
    assert str(broker)
