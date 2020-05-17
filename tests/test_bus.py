import uuid
import json
import asyncio
import pytest
import logging
from unittest.mock import MagicMock, AsyncMock
from microagent.bus import AbstractSignalBus
from microagent.signal import SignalException, SerializingError, Signal, Receiver  # , LookupKey

DSN = 'redis://localhost'


class Handler(AsyncMock):
    def __name__(self):
        return 'Handler'


@pytest.fixture()
def test_signal():
    return Signal(name='test_signal', providing_args=['uuid'])


@pytest.fixture()
def else_signal():
    return Signal(name='else_signal', providing_args=['uuid'])


async def test_Signal_register_ok(test_signal, else_signal):
    assert Signal.get_all()['test_signal'] is test_signal
    assert Signal.get_all()['else_signal'] is else_signal
    assert Signal.get_all()['test_signal'] == test_signal
    assert Signal.get_all()['else_signal'] == else_signal
    assert test_signal.name == 'test_signal'
    assert test_signal.providing_args == ['uuid']
    assert not test_signal == 1


async def test_Signal_repr_ok(test_signal):
    assert 'Signal' in str(test_signal)


async def test_Signal_get_ok(test_signal):
    assert Signal.get('test_signal') is test_signal


async def test_Signal_get_fail_not_found():
    with pytest.raises(SignalException):
        Signal.get('not_found')


async def test_Signal_make_channel_name_ok(test_signal):
    assert test_signal.make_channel_name('TEST') == 'TEST:test_signal:*'


async def test_Signal_serialize_ok(test_signal):
    assert test_signal.serialize({'a': 1}) == '{"a":1}'


async def test_Signal_serialize_fail(test_signal):
    with pytest.raises(SerializingError):
        test_signal.serialize({'a': pytest})


async def test_Signal_deserialize_ok(test_signal):
    assert test_signal.deserialize('{"a":1}') == {'a': 1}


async def test_Signal_deserialize_fail(test_signal):
    with pytest.raises(SerializingError):
        test_signal.deserialize('fail')


async def test_Receiver_ok(test_signal):
    receiver = Receiver(agent=None, handler=lambda: 1, signal=test_signal, timeout=60)
    assert 'Receiver' in str(receiver)
    assert receiver.agent is None
    assert receiver.handler() == 1
    assert receiver.signal is test_signal
    assert receiver.timeout == 60


class Bus(AbstractSignalBus):
    RESPONSE_TIMEOUT = 0.5

    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass


@pytest.fixture
async def bus(event_loop):
    bus = Bus(dsn=DSN, prefix='TEST')
    bus._loop = event_loop
    bus.bind = AsyncMock()
    bus.send = AsyncMock()
    bus.log = MagicMock()
    return bus


async def test_Bus_init_ok():
    bus = Bus(dsn=DSN, prefix='TEST')

    assert bus.dsn == DSN
    assert bus.prefix == 'TEST'

    await bus.send('channel', 'message')
    await bus.bind('channel')

    assert 'Bus' in str(bus)

    bus = Bus(dsn=DSN)

    assert bus.prefix == 'PUBSUB'

    logger = logging.getLogger('name')
    bus = Bus(dsn=DSN, logger=logger)
    assert bus.log is logger

    with pytest.raises(TypeError):
        Bus()  # noqa


async def test_Bus_bind_ok(bus, test_signal, else_signal):
    receiver1 = Receiver(agent=None, handler=Handler(), signal=test_signal, timeout=60)
    receiver2 = Receiver(agent=None, handler=Handler(), signal=test_signal, timeout=60)
    receiver3 = Receiver(agent=None, handler=Handler(), signal=else_signal, timeout=60)

    await bus.bind_receiver(receiver1)
    await bus.bind_receiver(receiver2)
    await bus.bind_receiver(receiver3)

    assert bus.receivers == {
        test_signal.name: [receiver1, receiver2],
        else_signal.name: [receiver3],
    }

    assert [x[0][0] for x in bus.bind.call_args_list] == [
        'TEST:test_signal:*', 'TEST:else_signal:*']


async def test_Bus_send_ok(bus, test_signal):
    await bus.test_signal.send(sender='test', uuid=1)
    bus.send.assert_called_once()
    assert bus.send.call_args[0][0] == 'TEST:test_signal:test'
    assert json.loads(bus.send.call_args[0][1]) == {'uuid': 1}


async def test_Bus_call_ok(bus, test_signal):
    def finish():
        for uid in bus.response_context._responses:
            bus.response_context.finish(uid, {'q': 42})

    loop = asyncio.get_running_loop()
    loop.call_later(0.01, finish)

    assert await bus.test_signal.call(sender='test', uuid=1) == {'q': 42}


async def test_Bus_receiver_ok(bus, test_signal):
    bus.handle_signal = AsyncMock()
    bus.receiver('TEST:test_signal:test', '{"uuid": 1}')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_called()
    bus.handle_signal.assert_called_with(test_signal, 'test', None, {"uuid": 1})


async def test_Bus_receiver_fail_bad_msg(bus, test_signal):
    bus.handle_signal = AsyncMock()
    bus.receiver('TEST:test_signal:test', 'fail')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_not_called()
    bus.log.error.assert_called()
    bus.receiver('TEST:test_signal:test', '[]')
    await asyncio.sleep(.001)
    bus.handle_signal.assert_not_called()
    assert bus.log.error.call_count == 2


async def test_Bus_receiver_ok_missed_args(bus, test_signal):
    bus.handle_signal = AsyncMock()
    bus.receiver('TEST:test_signal:test', '{"a": 1}')
    await asyncio.sleep(.001)
    bus.log.warning.assert_called()


async def test_Bus_response_ok(bus):
    uid = str(uuid.uuid4().hex)
    bus.response_context = MagicMock(finish=MagicMock())

    bus.receiver(f'TEST:response:test#{uid}', '{"uuid": 1}')
    await asyncio.sleep(.001)

    bus.response_context.finish.assert_called()
    bus.response_context.finish.assert_called_with(uid, {"uuid": 1})


async def test_Bus_handle_response_fail(bus):
    bus.response_context = MagicMock(finish=MagicMock())
    bus.response_context.finish.side_effect = asyncio.InvalidStateError()
    bus.handle_response(str(uuid.uuid4().hex), {'q': 1})
    bus.log.error.assert_called()


async def test_Bus_handle_signal_ok(bus):
    some_signal = Signal(name='some_signal', providing_args=[])
    receiver = Receiver(agent=None, handler=Handler(), signal=some_signal, timeout=60)
    bus.receivers[some_signal.name] = [receiver]
    bus.broadcast = AsyncMock()

    await bus.handle_signal(some_signal, 'test', None, {})

    bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})


async def test_Bus_handle_signal_ok_response(bus):
    uid = str(uuid.uuid4().hex)
    some_signal = Signal(name='some_signal', providing_args=[])
    handler = Handler(**{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=some_signal, timeout=60)
    bus.receivers[some_signal.name] = [receiver]
    bus.broadcast = AsyncMock(return_value=42)
    bus.send = AsyncMock()

    await bus.handle_signal(some_signal, 'test', uid, {})

    bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})
    bus.send.assert_called_with(
        f'{bus.prefix}:response:{bus.uid}#{uid}', f'{{"{receiver.key}":42}}')


async def test_Bus_broadcast_ok(bus, test_signal):
    handler = Handler(return_value=1, **{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=test_signal, timeout=60)
    assert await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1}) == 1


async def test_Bus_broadcast_ok_none(bus, test_signal):
    handler = Handler(return_value=None, **{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=test_signal, timeout=60)
    assert await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1}) is None


async def test_Bus_broadcast_ok_sync(bus, test_signal):
    handler = MagicMock(return_value=1, **{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=test_signal, timeout=60)
    assert await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1}) == 1


async def test_Bus_broadcast_fail_type_failed(bus, test_signal):
    handler = MagicMock(return_value=1, **{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=test_signal, timeout=60)
    receiver.handler.side_effect = TypeError()

    ret = await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})

    assert ret is None
    bus.log.error.assert_called()


async def test_Bus_broadcast_fail_timeout(bus, test_signal):
    handler = Handler(return_value=1, **{'__qualname__': 'qname'})
    receiver = Receiver(agent=None, handler=handler, signal=test_signal, timeout=.01)

    async def foo(*args, **kwargs):
        await asyncio.sleep(1)

    receiver.handler.side_effect = foo

    ret = await bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})

    assert ret is None
    bus.log.error.assert_called()


async def test_ResponseContext_ok(bus):
    async with bus.response_context(None, 1) as (signal_id, future):
        assert signal_id in bus.response_context._responses
        bus.response_context.finish(signal_id, {'q': 1})
        assert signal_id not in bus.response_context._responses
        assert await future == {'q': 1}


async def test_ResponseContext_ok_await_for(bus):
    async with bus.response_context('b', 1) as (signal_id, future):
        assert signal_id in bus.response_context._responses
        bus.response_context.finish(signal_id, {'a': 1})
        assert signal_id in bus.response_context._responses
        bus.response_context.finish(signal_id, {'b': 2})
        assert signal_id not in bus.response_context._responses
        assert await future == {'b': 2}


async def test_ResponseContext_fail_already_closed(bus):
    async with bus.response_context(None, 1) as (signal_id, future):
        bus.response_context._responses[signal_id].close()
        assert signal_id not in bus.response_context._responses
        bus.response_context.finish(signal_id, {'q': 1})

        with pytest.raises(asyncio.CancelledError):
            await future
