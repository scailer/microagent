import unittest
import asynctest


class AgentMock:
    def __init__(self, bus=None, broker=None, logger=None, settings=None):
        self.settings = settings or {}
        self.broker = broker
        self.bus = bus
        self.log = unittest.mock.MagicMock()
        self.start = asynctest.CoroutineMock()
        self.inited = True

    def assert_inited(self):
        if not getattr(self, 'inited', False):
            raise AssertionError('Agent is not initialized')


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_bus(*args, **kw):
    from microagent.tools.pulsar import RedisSignalBus

    bus = RedisSignalBus('redis://127.0.0.1:6379/7')

    bus.transport.publish = asynctest.CoroutineMock()
    bus.transport.psubscribe = asynctest.CoroutineMock()
    bus._receiver = unittest.mock.Mock()

    bus.transport.add_client.assert_called_once()
    bus.transport.add_client.assert_called_with(bus.receiver)

    bus.receiver('channel', 'message')
    bus._receiver.assert_called_with('channel', 'message')

    await bus.bind('channel')
    bus.transport.psubscribe.assert_called_with('channel')

    await bus.send('channel', 'message')
    bus.transport.publish.assert_called_with('channel', 'message')


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_broker(*args, **kw):
    from microagent.tools.pulsar import PulsarRedisBroker

    broker = PulsarRedisBroker('redis://127.0.0.1:6379/7')

    broker.redis_store.client.assert_called()
    broker.transport.rpush = asynctest.CoroutineMock()
    broker.transport.llen = asynctest.CoroutineMock(1)
    assert broker.redis_store.client.call_count == 1

    await broker.new_connection()
    assert broker.redis_store.client.call_count == 2

    await broker.send('name', 'message')
    broker.transport.rpush.assert_called_with('name', 'message')

    ret = await broker.queue_length('name')
    broker.transport.llen.assert_called_with('name')
    assert ret == 1


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_app_default(*args, **kw):
    from microagent.tools.pulsar import MicroAgentApp, RedisSignalBus

    worker = asynctest.MagicMock()

    app = MicroAgentApp()
    app.cfg.agent = AgentMock
    app.worker_start(worker)

    worker.agent.assert_inited()
    worker.agent.start.assert_called()
    assert isinstance(worker.agent.bus, RedisSignalBus)
    assert worker.agent.broker is None


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_app_no_bus_no_broker(*args, **kw):
    from microagent.tools.pulsar import MicroAgentApp, SignalBus

    worker = asynctest.MagicMock()

    app = MicroAgentApp()
    app.cfg.settings['signal_bus'] = SignalBus()
    app.cfg.settings['signal_bus'].set('')
    app.cfg.agent = AgentMock
    app.worker_start(worker)

    worker.agent.assert_inited()
    worker.agent.start.assert_called()
    assert worker.agent.bus is None
    assert worker.agent.broker is None


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_app_with_redis_broker(*args, **kw):
    from microagent.tools.pulsar import MicroAgentApp, QueueBroker, PulsarRedisBroker

    worker = asynctest.MagicMock()

    app = MicroAgentApp()
    app.cfg.settings['queue_broker'] = QueueBroker()
    app.cfg.settings['queue_broker'].set('redis://127.0.0.1:6379/7')
    app.cfg.agent = AgentMock
    app.worker_start(worker)

    worker.agent.assert_inited()
    worker.agent.start.assert_called()
    assert isinstance(worker.agent.broker, PulsarRedisBroker)


@asynctest.mock.patch('pulsar.apps.data.create_store')
async def test_app_with_amqp_broker(*args, **kw):
    from microagent.tools.pulsar import MicroAgentApp, QueueBroker
    from microagent.tools.amqp import AMQPBroker

    worker = asynctest.MagicMock()

    app = MicroAgentApp()
    app.cfg.settings['queue_broker'] = QueueBroker()
    app.cfg.settings['queue_broker'].set('amqp://fake')
    app.cfg.agent = AgentMock
    app.worker_start(worker)

    worker.agent.assert_inited()
    worker.agent.start.assert_called()
    assert isinstance(worker.agent.broker, AMQPBroker)
