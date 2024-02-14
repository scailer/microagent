# mypy: ignore-errors
import asyncio
import unittest

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from microagent import MicroAgent, Queue, Signal, consumer, cron, on, periodic, receiver
from microagent.agent import MissConfig
from microagent.tools.mocks import BrokerMock, BusMock


@pytest.fixture()
def test_queue():
    return Queue(name='test_queue')


@pytest.fixture()
def test_signal():
    return Signal(name='test_signal', providing_args=['uuid'])


@pytest.fixture()
def else_signal():
    return Signal(name='else_signal', providing_args=['uuid'])


@pytest.fixture
def dummy_receiver(test_signal, else_signal, test_queue):
    class DummyReceiverMicroAgent(MicroAgent):
        handler = AsyncMock()
        handler_sync = Mock()

        @receiver(test_signal)
        async def handler_one(self, uuid, **kw):
            await self.handler(kw)

        @receiver(test_signal, else_signal, timeout=10)
        async def handler_two(self, **kw):
            await self.handler(kw)

        @receiver(else_signal, timeout=5)
        def handler_three(self, **kw):
            self.handler_sync(kw)

    return DummyReceiverMicroAgent(bus=BusMock(), broker=BrokerMock())


@pytest.fixture
def dummy_consumer(test_queue):
    class DummyConsumerMicroAgent(MicroAgent):
        @consumer(test_queue, timeout=60, option1=1)
        def handler(self, **kw):
            pass

    return DummyConsumerMicroAgent(bus=BusMock(), broker=BrokerMock())


@pytest.fixture
def dummy_period():
    class DummyPeriodMicroAgent(MicroAgent):
        @periodic(period=60, timeout=55, start_after=30)
        def period_handler(self):
            pass

        @cron('0 * * * *', timeout=60)
        def cron_handler(self):
            pass

    return DummyPeriodMicroAgent(bus=BusMock(), broker=BrokerMock())


@pytest.fixture
def dummy_hook():
    class DummyHookMicroAgent(MicroAgent):
        @on('pre_start')
        def pre_setup(self):
            self._setup_pre_called = True

        @on('pre_start')
        def pre_start(self):
            self._start_pre_called = True

        @on('post_start')
        async def post_setup(self):
            self._setup_post_called = True

    return DummyHookMicroAgent(bus=BusMock(), broker=BrokerMock())


def test_init_empty_agent_ok():
    ma = MicroAgent()
    assert ma.receivers == []
    assert ma.periodic_tasks == []
    assert ma.cron_tasks == []
    assert ma.consumers == []
    assert ma.hook.binds == {}
    assert ma.settings == {}
    assert 'MicroAgent' in str(ma)


def test_init_logger_agent_ok():
    logger = unittest.mock.Mock()
    ma = MicroAgent(log=logger)
    assert ma.log is logger


async def test_init_receiver_agent_ok(dummy_receiver, test_signal, else_signal):
    assert len(dummy_receiver.receivers) == 4
    assert dummy_receiver.periodic_tasks == []
    assert dummy_receiver.cron_tasks == []
    assert dummy_receiver.consumers == []
    assert dummy_receiver.hook.binds == {}
    assert 'MicroAgent' in str(dummy_receiver)
    assert dummy_receiver.info()

    handlers = {(x.key, x.signal.name): x for x in dummy_receiver.receivers}
    key = 'dummy_receiver.<locals>.DummyReceiverMicroAgent'

    handler_one = handlers[(f'{key}.handler_one', 'test_signal')]
    handler_two_1 = handlers[(f'{key}.handler_two', 'test_signal')]
    handler_two_2 = handlers[(f'{key}.handler_two', 'else_signal')]
    handler_three = handlers[(f'{key}.handler_three', 'else_signal')]

    assert handler_one.agent == dummy_receiver
    assert handler_one.handler == dummy_receiver.handler_one
    assert handler_one.signal == test_signal
    assert handler_one.timeout == 60

    assert handler_two_1.agent == dummy_receiver
    assert handler_two_1.handler == dummy_receiver.handler_two
    assert handler_two_1.signal == test_signal
    assert handler_two_1.timeout == 10

    assert handler_two_2.agent == dummy_receiver
    assert handler_two_2.handler == dummy_receiver.handler_two
    assert handler_two_2.signal == else_signal
    assert handler_two_2.timeout == 10

    assert handler_three.agent == dummy_receiver
    assert handler_three.handler == dummy_receiver.handler_three
    assert handler_three.signal == else_signal
    assert handler_three.timeout == 5


async def test_init_receiver_agent_fail(test_signal):
    class FailedReceiverMicroAgent(MicroAgent):
        @receiver(test_signal)
        async def handler_one(self, uuid, **kw):
            pass

    with pytest.raises(MissConfig):
        FailedReceiverMicroAgent()

    with pytest.raises(MissConfig):
        FailedReceiverMicroAgent(bus=1)


async def test_init_consumer_agent_ok(dummy_consumer, test_queue):
    assert len(dummy_consumer.consumers) == 1
    assert dummy_consumer.receivers == []
    assert dummy_consumer.periodic_tasks == []
    assert dummy_consumer.cron_tasks == []
    assert dummy_consumer.hook.binds == {}
    assert 'MicroAgent' in str(dummy_consumer)
    assert dummy_consumer.info()

    handler = dummy_consumer.consumers[0]

    assert handler.queue == test_queue
    assert handler.timeout == 60
    assert handler.options == {'option1': 1}
    assert handler.handler == dummy_consumer.handler


async def test_init_consumer_agent_fail(test_queue):
    class FailedConsumerMicroAgent(MicroAgent):
        @consumer(test_queue)
        async def handler_one(self, uuid, **kw):
            pass

    with pytest.raises(MissConfig):
        FailedConsumerMicroAgent()

    with pytest.raises(MissConfig):
        FailedConsumerMicroAgent(broker=1)


async def test_init_periodic_agent_ok(dummy_period):
    assert dummy_period.consumers == []
    assert dummy_period.receivers == []
    assert len(dummy_period.periodic_tasks) == 1
    assert len(dummy_period.cron_tasks) == 1
    assert dummy_period.hook.binds == {}
    assert 'MicroAgent' in str(dummy_period)
    assert dummy_period.info()

    phandler = dummy_period.periodic_tasks[0]

    assert phandler.handler == dummy_period.period_handler
    assert phandler.period == 60
    assert phandler.timeout == 55
    assert phandler.start_after == 30

    chandler = dummy_period.cron_tasks[0]

    assert chandler.handler == dummy_period.cron_handler
    assert chandler.timeout == 60
    assert chandler.cron.minutes == [0]


async def test_init_hook_agent_ok(dummy_hook):
    assert dummy_hook.consumers == []
    assert dummy_hook.receivers == []
    assert dummy_hook.periodic_tasks == []
    assert dummy_hook.cron_tasks == []
    assert len(dummy_hook.hook.binds) == 2
    assert len(dummy_hook.hook.binds['pre_start']) == 2
    assert len(dummy_hook.hook.binds['post_start']) == 1
    assert 'MicroAgent' in str(dummy_hook)
    assert dummy_hook.info()

    post_start = dummy_hook.hook.binds['post_start'][0]
    assert post_start.handler == dummy_hook.post_setup

    pre_start1, pre_start2 = dummy_hook.hook.binds['pre_start']
    assert {pre_start1.handler, pre_start2.handler} == {dummy_hook.pre_setup, dummy_hook.pre_start}


async def test_start_receiver_agent_ok(dummy_receiver):
    await dummy_receiver.start()

    data = set(
        (x.args[0].key, x.args[0].signal.name)
        for x in dummy_receiver.bus.bind_receiver.call_args_list
    )

    assert set((x.key, x.signal.name) for x in dummy_receiver.receivers) == data


async def test_start_receiver_agent_disabled_ok(dummy_receiver):
    await dummy_receiver.start(enable_receiving_signals=False)
    dummy_receiver.bus.bind_receiver.assert_not_called()


async def test_start_consumer_agent_ok(dummy_consumer):
    await dummy_consumer.start()

    data = set(
        x.args[0].queue.name for x in dummy_consumer.broker.bind_consumer.call_args_list
    )

    assert set(x.queue.name for x in dummy_consumer.consumers) == data


async def test_start_consumer_agent_disabled_ok(dummy_consumer):
    await dummy_consumer.start(enable_consuming_messages=False)
    dummy_consumer.broker.bind_consumer.assert_not_called()


async def test_start_period_agent_ok(dummy_period):
    start1, start2 = Mock(), Mock()
    dummy_period.periodic_tasks = [MagicMock(start=start1, start_after=None)]
    dummy_period.cron_tasks = [MagicMock(start=start2, start_after=1000)]

    await dummy_period.start()

    start1.assert_called_once()
    start2.assert_called_once()


async def test_start_period_agent_disabled_ok(dummy_period):
    start1, start2 = Mock(), Mock()
    dummy_period.periodic_tasks = [MagicMock(start=start1)]
    dummy_period.cron_tasks = [MagicMock(start=start2)]

    await dummy_period.start(enable_periodic_tasks=False)

    start1.assert_not_called()
    start2.assert_not_called()


@pytest.fixture
def dummy_server():
    class DummyServerMicroAgent(MicroAgent):
        _server_called = False

        @on('server')
        async def run_server(self):
            self._server_called = True

    return DummyServerMicroAgent()


async def test_start_hook_server_ok(dummy_server):
    await dummy_server.start()
    await asyncio.sleep(.01)

    assert dummy_server._server_called


async def test_start_hook_server_disabled_ok(dummy_server):
    await dummy_server.start(enable_servers_running=False)
    await asyncio.sleep(.01)

    assert not dummy_server._server_called


async def test_start_hook_start_agent_ok(dummy_hook):
    dummy_hook.hook = MagicMock(pre_start=AsyncMock(), post_start=AsyncMock())

    await dummy_hook.start()

    dummy_hook.hook.pre_start.assert_called_once()
    dummy_hook.hook.post_start.assert_called_once()


async def test_start_hook_stop_agent_ok(dummy_hook):
    dummy_hook.hook = MagicMock(pre_stop=AsyncMock())

    await dummy_hook.stop()

    dummy_hook.hook.pre_stop.assert_called_once()
