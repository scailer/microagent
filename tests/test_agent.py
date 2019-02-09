import asyncio
import unittest
import asynctest
from unittest.mock import Mock
from microagent import MicroAgent, Signal, Queue, receiver, consumer, periodic, cron

test_signal = Signal(name='test_signal', providing_args=['uuid'])
else_signal = Signal(name='else_signal', providing_args=['uuid'])
test_queue = Queue(name='test_queue')


class DummyReceiverMicroAgent(MicroAgent):
    @receiver(test_signal)
    def handler(self, **kw):
        pass

    @receiver(test_signal, 'else_signal')
    def else_handler(self, **kw):
        pass


class DummyPeriodMicroAgent(MicroAgent):
    def setup(self):
        self._setup_called = True

    @periodic(period=60, timeout=60, start_after=60)
    def handler(self, **kw):
        pass

    @cron('0 * * * *', timeout=60)
    def cron_handler(self, **kw):
        pass


class DummyQueueMicroAgent(MicroAgent):
    @consumer(test_queue, timeout=60)
    def handler(self, **kw):
        pass


class TestAgent(asynctest.TestCase):
    def test_init_empty(self):
        ma = MicroAgent()
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma._periodic_tasks, ())
        self.assertEqual(ma.received_signals, {})

    def test_init_logger(self):
        logger = unittest.mock.Mock()
        ma = MicroAgent(logger=logger)
        self.assertEqual(ma.log, logger)

    def test_init_periodic(self):
        ma = DummyPeriodMicroAgent()
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma.queue_consumers, ())
        self.assertEqual(ma.received_signals, {})
        self.assertIn(ma.handler, ma._periodic_tasks)
        self.assertIn(ma.cron_handler, ma._periodic_tasks)
        self.assertTrue(ma._setup_called)

    async def test_init_bus(self):
        bus = asynctest.MagicMock()
        bus.bind_signal = asynctest.CoroutineMock()
        ma = DummyReceiverMicroAgent(bus=bus)
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma._periodic_tasks, ())
        self.assertEqual(ma.queue_consumers, ())
        self.assertIn(test_signal.name, ma.received_signals)
        self.assertIn(else_signal.name, ma.received_signals)
        await asyncio.sleep(.001)
        self.assertEqual(bus.bind_signal.call_count, 2)
        bus.bind_signal.assert_any_call(test_signal)
        bus.bind_signal.assert_any_call(else_signal)

    async def test_init_bus_disabled(self):
        bus = asynctest.MagicMock()
        ma = DummyReceiverMicroAgent(bus=bus, enable_receiving_signals=False)
        bus.bind_signal.assert_not_called()

    async def test_init_no_bus(self):
        self.assertRaises(AssertionError, DummyReceiverMicroAgent)

    async def test_init_queue(self):
        broker = asynctest.MagicMock()
        ma = DummyQueueMicroAgent(broker=broker)
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma._periodic_tasks, ())
        self.assertEqual(ma.received_signals, {})
        self.assertIn(ma.handler, ma.queue_consumers)
        await asyncio.sleep(.001)
        broker.bind_consumer.assert_called_once()
        broker.bind_consumer.assert_called_with(ma.handler)
        self.assertEqual(test_queue, ma.handler.queue)

    async def test_init_queue_disabled(self):
        broker = asynctest.MagicMock()
        ma = DummyQueueMicroAgent(broker=broker, enable_consuming_messages=False)
        broker.bind_consumer.assert_not_called()

    async def test_init_no_queue(self):
        self.assertRaises(AssertionError, DummyQueueMicroAgent)

    def test_info(self):
        ma = MicroAgent(Mock())
        self.assertTrue(bool(ma.info()))
