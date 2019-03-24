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
    @on('pre_start')
    def pre_setup(self):
        self._setup_pre_called = True

    @on('post_start')
    async def post_setup(self):
        self._setup_post_called = True

    @periodic(period=60, timeout=60, start_after=0)
    def periodic_handler(self, **kw):
        self._periodic_handler_called = True

    @on('pre_periodic_handler')
    def pre_periodic_handler(self, *args, **kwargs):
        self._pre_periodic_called = True

    @on('post_periodic_handler')
    async def post_periodic_handler(self, *args, **kwargs):
        self._post_periodic_called = True

    @receiver(test_signal)
    async def receiver_handler(self, **kw):
        self._receiver_handler_called = True

    @on('pre_receiver_handler')
    def pre_receiver_handler(self, *args, **kwargs):
        self._pre_receiver_called = True

    @on('post_receiver_handler')
    async def post_receiver_handler(self, *args, **kwargs):
        self._post_receiver_called = True

    @consumer(test_queue, timeout=60)
    async def consumer_handler(self, **kw):
        self._consumer_handler_called = True

    @on('pre_consumer_handler')
    def pre_consumer_handler(self, *args, **kwargs):
        self._pre_consumer_called = True

    @on('post_consumer_handler')
    async def post_consumer_handler(self, *args, **kwargs):
        self._post_consumer_called = True

    @receiver(else_signal)
    async def receiver_handler_failed(self, **kw):
        raise Exception('receiver_handler_failed')

    @on('error_receiver_handler_failed')
    def err_receiver_handler_failed(self, *args, **kw):
        self._error_receiver_called = True


async def test_hooks_ok():
    ma = DummyHookMicroAgent(bus=BusMock(), broker=BrokerMock())
    await ma.start()

    assert ma._setup_pre_called
    assert ma._setup_post_called

    await asyncio.sleep(.01)

    assert ma._periodic_handler_called
    assert ma._pre_periodic_called
    assert ma._post_periodic_called

    await ma.hook.decorate(ma.receiver_handler)(signal=test_signal)

    assert ma._receiver_handler_called
    assert ma._pre_receiver_called
    assert ma._post_receiver_called

    await ma.hook.decorate(ma.consumer_handler)()

    assert ma._consumer_handler_called
    assert ma._pre_consumer_called
    assert ma._post_consumer_called

    with pytest.raises(Exception):
        await ma.hook.decorate(ma.receiver_handler_failed)(signal=else_signal)

    assert ma._error_receiver_called

    with pytest.raises(AttributeError):
        ma.hook.no_data


def test_init_empty_ok():
    ma = MicroAgent()
    assert ma.settings == {}
    assert ma._periodic_tasks == ()
    assert ma.received_signals == {}


def test_init_logger_ok():
    logger = unittest.mock.Mock()
    ma = MicroAgent(logger=logger)
    assert ma.log == logger


async def test_init_periodic_ok():
    ma = DummyPeriodMicroAgent()
    await ma.start()

    assert ma.settings == {}
    assert ma.queue_consumers == ()
    assert ma.received_signals == {}

    periodic_tasks = [x.origin.__name__ for x in ma._periodic_tasks]
    assert ma.handler.__name__ in periodic_tasks
    assert ma.cron_handler.__name__ in periodic_tasks
    assert ma._setup_called
    assert set(x['name'] for x in ma.info()['periodic']) == set(periodic_tasks)


async def test_init_bus_ok():
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
    assert len(ma.info()['receivers']) == len(ma.received_signals)


async def test_init_bus_disabled_ok():
    bus = BusMock()
    ma = DummyReceiverMicroAgent(bus=bus)
    await ma.start(enable_receiving_signals=False)
    bus.bind_signal.assert_not_called()


async def test_init_no_bus_ok():
    with pytest.raises(AssertionError):
        DummyReceiverMicroAgent()


async def test_init_queue_ok():
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
    assert len(ma.info()['consumers']) == len(queue_consumers)


async def test_init_queue_disabled():
    broker = BrokerMock()
    ma = DummyQueueMicroAgent(broker=broker)
    await ma.start(enable_consuming_messages=False)
    broker.bind_consumer.assert_not_called()


async def test_init_no_queue():
    with pytest.raises(AssertionError):
        DummyQueueMicroAgent()


async def test_stop():
    ma = MicroAgent(Mock())
    await ma.stop()


async def test_repr():
    ma = MicroAgent(Mock())
    assert MicroAgent.__name__ in str(ma)
