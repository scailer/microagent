import pytest
import asyncio
import unittest
from unittest.mock import Mock
from microagent import MicroAgent, Signal, Queue, receiver, consumer, periodic, cron, on
from microagent.tools.mocks import BusMock, BrokerMock

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
    @on('pre_start')
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


class DummyHookMicroAgent(MicroAgent):

    @periodic(period=60, timeout=60, start_after=0)
    def periodic_handler(self, **kw):
        self._handler_called = True

    @on('pre_periodic_handler')
    def pre_handle(self, *args, **kwargs):
        self._pre_called = True

    @on('post_periodic_handler')
    def post_handle(self, *args, **kwargs):
        self._post_called = True


def test_init_empty():
    ma = MicroAgent()
    assert ma.settings == {}
    assert ma._periodic_tasks == ()
    assert ma.received_signals == {}


def test_init_logger():
    logger = unittest.mock.Mock()
    ma = MicroAgent(logger=logger)
    assert ma.log == logger


async def test_init_periodic():
    ma = DummyPeriodMicroAgent()
    await ma.start()

    assert ma.settings == {}
    assert ma.queue_consumers == ()
    assert ma.received_signals == {}

    periodic_tasks = [x.origin.__name__ for x in ma._periodic_tasks]
    assert ma.handler.__name__ in periodic_tasks
    assert ma.cron_handler.__name__ in periodic_tasks
    assert ma._setup_called


async def test_hooks_ok():
    ma = DummyHookMicroAgent()
    await ma.start()
    await asyncio.sleep(.01)

    assert ma.settings == {}
    assert ma.queue_consumers == ()
    assert ma.received_signals == {}

    periodic_tasks = [x.origin.__name__ for x in ma._periodic_tasks]
    assert ma.periodic_handler.__name__ in periodic_tasks
    assert ma._handler_called
    assert ma._pre_called
    assert ma._post_called


async def test_init_bus():
    bus = BusMock()
    ma = DummyReceiverMicroAgent(bus=bus)

    assert ma.settings == {}
    assert ma._periodic_tasks == ()
    assert ma.queue_consumers == ()
    assert test_signal.name in ma.received_signals
    assert else_signal.name in ma.received_signals

    await ma.start()

    assert bus.bind_signal.call_count == 2
    bus.bind_signal.assert_any_call(test_signal)
    bus.bind_signal.assert_any_call(else_signal)


async def test_init_bus_disabled():
    bus = BusMock()
    ma = DummyReceiverMicroAgent(bus=bus)
    await ma.start(enable_receiving_signals=False)
    bus.bind_signal.assert_not_called()


async def test_init_no_bus():
    with pytest.raises(AssertionError):
        DummyReceiverMicroAgent()


async def test_init_queue():
    broker = BrokerMock()
    ma = DummyQueueMicroAgent(broker=broker)

    assert ma.settings == {}
    assert ma._periodic_tasks == ()
    assert ma.received_signals == {}
    queue_consumers = [x.__name__ for x in ma.queue_consumers]
    assert ma.handler.__name__ in queue_consumers
    assert ma.handler.__name__ in queue_consumers

    await ma.start()

    broker.bind_consumer.assert_called_once()
    broker.bind_consumer.assert_called_with(ma.handler)
    assert test_queue == ma.handler.queue


async def test_init_queue_disabled():
    broker = BrokerMock()
    ma = DummyQueueMicroAgent(broker=broker)
    await ma.start(enable_consuming_messages=False)
    broker.bind_consumer.assert_not_called()


async def test_init_no_queue():
    with pytest.raises(AssertionError):
        DummyQueueMicroAgent()


def test_info():
    ma = MicroAgent(Mock())
    assert ma.info()


async def test_stop():
    ma = MicroAgent(Mock())
    await ma.stop()
