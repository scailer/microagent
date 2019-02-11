import uuid
import json
import asyncio
import unittest
import asynctest
from microagent.bus import AbstractSignalBus, ResponseContext
from microagent.signal import SignalException
from microagent import Signal


test_signal = Signal(name='test_signal', providing_args=['uuid'])
else_signal = Signal(name='else_signal', providing_args=['uuid'])


class Bus(AbstractSignalBus):
    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass

    def receiver(self, *args, **kwargs):
        pass


class TestSignal(unittest.TestCase):
    def test_repr(self):
        self.assertIn('Signal', str(test_signal))

    def test_not_found(self):
        self.assertRaises(SignalException, Signal.get, 'not_found')


class TestBus(asynctest.TestCase):
    def setUp(self):
        self.dsn = 'redis://localhost'
        self.bus = Bus(dsn=self.dsn, prefix='TEST')
        self.bus.bind = asynctest.CoroutineMock()
        self.bus.send = asynctest.CoroutineMock()
        self.bus.receiver = asynctest.CoroutineMock()


    def test_init(self):
        self.assertEqual(self.bus.dsn, self.dsn)
        self.assertEqual(self.bus.prefix, 'TEST')
        self.bus.send('channel', 'message')
        self.bus.bind('channel')
        self.bus.receiver()
        self.assertIn('Bus', str(self.bus))

    async def test_bind(self):
        test_signal.receivers = [asynctest.CoroutineMock()]

        await self.bus.bind_signal(test_signal)
        self.bus.bind.assert_called_once()
        self.assertIn(test_signal.name, self.bus.received_signals)

        await self.bus.bind_signal(test_signal)
        self.bus.bind.assert_called_once()
        self.assertEqual(len(self.bus.received_signals[test_signal.name].receivers), 2)

    async def test_send(self):
        await self.bus.test_signal.send(sender='test', uuid=1)
        self.bus.send.assert_called_once()
        self.assertEqual(self.bus.send.call_args[0][0], 'TEST:test_signal:test')
        self.assertEqual(json.loads(self.bus.send.call_args[0][1]), {'uuid': 1})

    async def test_call(self):
        asyncio.get_event_loop().call_later(
            0.005, lambda: [ResponseContext.set(uid, 42) for uid in ResponseContext._responses])
        ret = await self.bus.test_signal.call(sender='test', uuid=1)
        self.assertEqual(ret, 42)

    async def test_receiver(self):
        self.bus.handle_signal = asynctest.CoroutineMock()
        self.bus.received_signals[test_signal.name] = test_signal
        self.bus._receiver('TEST:test_signal:test', '{"uuid": 1}')
        await asyncio.sleep(.001)
        self.bus.handle_signal.assert_called()
        self.bus.handle_signal.assert_called_with(test_signal, 'test', None, {"uuid": 1})

    async def test_receiver_bad_msg(self):
        self.bus.handle_signal = asynctest.CoroutineMock()
        self.bus.log = asynctest.CoroutineMock()
        self.bus.received_signals[test_signal.name] = test_signal
        self.bus._receiver('TEST:test_signal:test', 'fail')
        await asyncio.sleep(.001)
        self.bus.handle_signal.assert_not_called()
        self.bus.log.error.assert_called()
        self.bus._receiver('TEST:test_signal:test', '[]')
        await asyncio.sleep(.001)
        self.bus.handle_signal.assert_not_called()
        self.assertEqual(self.bus.log.error.call_count, 2)

    async def test_receiver_missed_args(self):
        self.bus.handle_signal = asynctest.CoroutineMock()
        self.bus.log = asynctest.CoroutineMock()
        self.bus.received_signals[test_signal.name] = test_signal
        self.bus._receiver('TEST:test_signal:test', '{"a": 1}')
        await asyncio.sleep(.001)
        self.bus.log.warning.assert_called()

    async def test_response(self):
        uid = str(uuid.uuid4().hex)
        self.bus.handle_response = asynctest.CoroutineMock()
        self.bus._receiver(f'TEST:response:test#{uid}', '{"uuid": 1}')
        self.bus.handle_response.assert_called()
        self.bus.handle_response.assert_called_with(uid, {"uuid": 1})

    async def test_handle_signal(self):
        receiver = asynctest.CoroutineMock()
        some_signal = Signal(name='some_signal', providing_args=[])
        some_signal.receivers = [('key', receiver)]
        self.bus.broadcast = asynctest.CoroutineMock()
        await self.bus.handle_signal(some_signal, 'test', None, {})
        self.bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})

    async def test_handle_response_signal(self):
        uid = str(uuid.uuid4().hex)
        receiver = unittest.mock.Mock(**{'__qualname__': 'qname'})
        some_signal = Signal(name='some_signal', providing_args=[])
        some_signal.receivers = [('key', receiver)]
        self.bus.broadcast = asynctest.CoroutineMock()
        self.bus.send = asynctest.CoroutineMock()
        await self.bus.handle_signal(some_signal, 'test', uid, {})
        self.bus.broadcast.assert_called_with(receiver, some_signal, 'test', {})
        self.bus.send.assert_called_with(
            f'{self.bus.prefix}:response:{self.bus.uid}#{uid}', '{"qname":null}')

    async def test_broadcast(self):
        receiver = asynctest.CoroutineMock(
            return_value=1, **{'timeout': 1, '__qualname__': 'qname'})
        ret = await self.bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
        self.assertEqual(ret, 1)

    async def test_broadcast_type_failed(self):
        self.bus.log = unittest.mock.Mock()
        receiver = unittest.mock.Mock(
            return_value=1, **{'timeout': 1, '__qualname__': 'qname'})
        receiver.side_effect = TypeError()
        ret = await self.bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
        self.assertEqual(ret, None)
        self.bus.log.error.assert_called()

    async def test_broadcast_timeout(self):
        self.bus.log = unittest.mock.Mock()
        receiver = asynctest.CoroutineMock(
            return_value=1, **{'timeout': .01, '__qualname__': 'qname'})

        async def foo(*args, **kwargs):
            await asyncio.sleep(1)

        receiver.side_effect = foo
        ret = await self.bus.broadcast(receiver, test_signal, 'test', {'uuid': 1})
        self.assertEqual(ret, None)
        self.bus.log.fatal.assert_called()
