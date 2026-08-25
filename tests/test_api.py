# mypy: ignore-errors
import json
import os
import tempfile

from pathlib import Path

import pytest

from microagent import (  # noqa
    DoubleLoadError,
    MicroAgent,
    Queue,
    Signal,
    __version__,
    configure,
    consumer,
    cron,
    load_queues,
    load_signals,
    load_stuff,
    periodic,
    receiver,
)
from microagent.bus import AbstractSignalBus
from microagent.tools import mocks


def test_load_from_file():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    signals, queues = load_stuff(source)
    assert len(signals) == 3
    assert len(queues) == 3
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
    assert len(queues) == 3
    assert queues.test_queue.name == 'test_queue'
    assert queues.push1.name == 'push1'
    assert queues.push1.exchange == 'ex'
    assert queues.push2.name == 'push2'
    assert queues.push2.exchange == 'ex'


def test_double_load_raises():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    load_stuff(source)
    with pytest.raises(DoubleLoadError, match='already loaded'):
        load_stuff(source)


def test_configure():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    assert configure(source) is None
    assert Signal.test_signal.name == 'test_signal'
    assert Queue.test_queue.name == 'test_queue'


def test_double_configure_raises():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    configure(source)
    with pytest.raises(DoubleLoadError, match='already loaded'):
        configure(source)


def test_signal_attribute_access():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    load_stuff(source)
    assert Signal.test_signal.name == 'test_signal'
    assert Signal.else_signal.name == 'else_signal'
    assert Signal.typed_signal.name == 'typed_signal'


def test_signal_attribute_access_nonexistent():
    with pytest.raises(AttributeError, match='not registered'):
        Signal.nonexistent_signal  # noqa


def test_queue_attribute_access():
    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    load_stuff(source)
    assert Queue.test_queue.name == 'test_queue'
    assert Queue.push1.name == 'push1'
    assert Queue.push2.name == 'push2'


def test_queue_attribute_access_nonexistent():
    with pytest.raises(AttributeError, match='not registered'):
        Queue.nonexistent_queue  # noqa


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


async def test_default_prefix_from_json():
    from microagent import Signal, Queue

    class TestBus(AbstractSignalBus):
        async def send(self, channel, message): pass
        async def bind(self, channel): pass

    source = 'file://' + str(Path(__file__).parent / 'stuff.json')
    configure(source)

    b1 = TestBus(dsn='redis://localhost')
    assert b1.prefix == 'PUBSUB'

    # test with default_prefix in json
    json_content = {
        'default_prefix': 'MYAPP',
        'signals': [
            {'name': 'other_signal', 'providing_args': []}
        ],
        'queues': []
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf8') as f:
        json.dump(json_content, f)
        f.flush()
        tmp_path = f.name

    Signal._signals = {}
    Queue._queues = {}
    configure('file://' + tmp_path)
    b2 = TestBus(dsn='redis://localhost')
    assert b2.prefix == 'MYAPP'

    # explicit prefix overrides
    b3 = TestBus(dsn='redis://localhost', prefix='EXPLICIT')
    assert b3.prefix == 'EXPLICIT'

    os.unlink(tmp_path)
