import pytest
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
    assert ma.handler in ma._periodic_tasks
    assert ma.cron_handler in ma._periodic_tasks
    assert ma._setup_called


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
    assert ma.handler in ma.queue_consumers

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
