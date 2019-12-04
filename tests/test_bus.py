import uuid
import json
import asyncio
import unittest
import asynctest
import pytest
from microagent.bus import AbstractSignalBus
from microagent.signal import SignalException, LookupKey
from microagent import Signal

DSN = 'redis://localhost'

test_signal = Signal(name='test_signal', providing_args=['uuid'])
else_signal = Signal(name='else_signal', providing_args=['uuid'])


class Bus(AbstractSignalBus):
    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass

    def receiver(self, *args, **kwargs):
        pass


def test_repr():
    assert 'Signal' in str(test_signal)


def test_not_found():
    with pytest.raises(SignalException):
        Signal.get('not_found')


@pytest.fixture
def bus(event_loop):
    bus = Bus(dsn=DSN, prefix='TEST')
    bus._loop = event_loop
    bus.bind = asynctest.CoroutineMock()
    bus.send = asynctest.CoroutineMock()
    bus.receiver = asynctest.CoroutineMock()
    return bus


def test_init(bus):
    assert bus.dsn == DSN
    assert bus.prefix == 'TEST'

    bus.send('channel', 'message')
    bus.bind('channel')
    bus.receiver()

    assert 'Bus' in str(bus)


async def test_bind(bus):
    test_signal.receivers = [(
        LookupKey(mod='mod', name='name', id=1),
        asynctest.CoroutineMock()
    )]

    await bus.bind_signal(test_signal)
    bus.bind.assert_called_once()
    assert test_signal.name in bus.received_signals

    await bus.bind_signal(test_signal)
    bus.bind.assert_called_once()
    assert len(bus.received_signals[test_signal.name].receivers) == 2


async def test_send(bus):
    await bus.test_signal.send(sender='test', uuid=1)
    bus.send.assert_called_once()
    assert bus.send.call_args[0][0] == 'TEST:test_signal:test'
    assert json.loads(bus.send.call_args[0][1]) == {'uuid': 1}


async def test_call(bus, event_loop):
    def finish():
        for uid in bus.response_context._responses:
            bus.response_context.finish(uid, 42)

    event_loop.call_later(0.01, finish)

    ret = await bus.test_signal.call(sender='test', uuid=1)

    assert ret == 42


async def test_receiver(bus):
    bus.handle_signal = asynctest.CoroutineMock()
    bus.received_signals[test_signal.name] = test_signal
    bus._receiver('TEST:test_signal:test', '{"uuid": 1}')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_called()
    bus.handle_signal.assert_called_with(test_signal, 'test', None, {"uuid": 1})


async def test_receiver_bad_msg(bus):
    bus.handle_signal = asynctest.CoroutineMock()
    bus.log = asynctest.CoroutineMock()
    bus.received_signals[test_signal.name] = test_signal
    bus._receiver('TEST:test_signal:test', 'fail')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_not_called()
    bus.log.error.assert_called()
    bus._receiver('TEST:test_signal:test', '[]')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_not_called()
    assert bus.log.error.call_count == 2


async def test_receiver_missed_args(bus):
    bus.handle_signal = asynctest.CoroutineMock()
    bus.log = asynctest.CoroutineMock()
    bus.received_signals[test_signal.name] = test_signal
    bus._receiver('TEST:test_signal:test', '{"a": 1}')
    await asyncio.sleep(.001)
    bus.log.warning.assert_called()


async def test_response(bus):
    uid = str(uuid.uuid4().hex)
    bus.handle_response = asynctest.CoroutineMock()
    bus._receiver(f'TEST:response:test#{uid}', '{"uuid": 1}')
    bus.handle_response.assert_called()
    bus.handle_response.assert_called_with(uid, {"uuid": 1})


async def test_handle_signal(bus):
    receiver = asynctest.CoroutineMock()
    some_signal = Signal(name='some_signal', providing_args=[])
    some_signal.receivers = [('key', receiver)]
    bus.broadcast = asynctest.CoroutineMock()
    await bus.handle_signal(some_signal, 'test', None, {})
    bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})


async def test_handle_response_signal(bus):
    uid = str(uuid.uuid4().hex)
    receiver = unittest.mock.Mock(**{'__qualname__': 'qname'})
    some_signal = Signal(name='some_signal', providing_args=[])
    some_signal.receivers = [('key', receiver)]
    bus.broadcast = asynctest.CoroutineMock()
    bus.send = asynctest.CoroutineMock()
    await bus.handle_signal(some_signal, 'test', uid, {})
    bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})
    bus.send.assert_called_with(
        f'{bus.prefix}:response:{bus.uid}#{uid}', '{"qname":null}')


async def test_broadcast(bus):
    receiver = asynctest.CoroutineMock(
        return_value=1, **{'timeout': 1, '__qualname__': 'qname'})
    ret = await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
    assert ret == 1


async def test_broadcast_type_failed(bus):
    bus.log = unittest.mock.Mock()
    receiver = unittest.mock.Mock(
        return_value=1, **{'timeout': 1, '__qualname__': 'qname'})
    receiver.side_effect = TypeError()
    ret = await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
    assert ret is None
    bus.log.error.assert_called()


async def test_broadcast_timeout(bus):
    bus.log = unittest.mock.Mock()
    receiver = asynctest.CoroutineMock(
        return_value=1, **{'timeout': .01, '__qualname__': 'qname'})

    async def foo(*args, **kwargs):
        await asyncio.sleep(1)

    receiver.side_effect = foo
    ret = await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
    assert ret is None
    bus.log.fatal.assert_called()
