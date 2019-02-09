import asyncio
import unittest
import asynctest
from unittest.mock import Mock

class TestBusBroker(asynctest.TestCase):
    @asynctest.mock.patch('pulsar.apps.data.create_store')
    async def test_bus(self, *args, **kw):
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
    async def test_broker(self, *args, **kw):
        from microagent.tools.pulsar import PulsarRedisBroker
        broker = PulsarRedisBroker('redis://127.0.0.1:6379/7')
        broker.redis_store.client.assert_called()
        broker.transport.rpush = asynctest.CoroutineMock()
        broker.transport.llen = asynctest.CoroutineMock(1)
        self.assertEqual(broker.redis_store.client.call_count, 1)

        await broker.new_connection()
        self.assertEqual(broker.redis_store.client.call_count, 2)

        await broker.send('name', 'message')
        broker.transport.rpush.assert_called_with('name', 'message')

        ret = await broker.queue_length('name')
        broker.transport.llen.assert_called_with('name')
        self.assertEqual(ret, 1)


class TestApp(asynctest.TestCase):
    @asynctest.mock.patch('pulsar.apps.data.create_store')
    async def test_app_default(self, *args, **kw):
        from microagent.tools.pulsar import MicroAgentApp, RedisSignalBus
        app = MicroAgentApp()
        worker = asynctest.MagicMock()
        app.cfg.agent = unittest.mock.MagicMock()
        app.worker_start(worker)
        app.cfg.agent.assert_called()
        args, kwargs = app.cfg.agent.call_args
        self.assertIsInstance(kwargs['bus'], RedisSignalBus)
        self.assertEqual(kwargs['broker'], None)

    @asynctest.mock.patch('pulsar.apps.data.create_store')
    async def test_app_no_bus_no_broker(self, *args, **kw):
        from microagent.tools.pulsar import MicroAgentApp, SignalBus
        app = MicroAgentApp()
        app.cfg.settings['signal_bus'] = SignalBus()
        app.cfg.settings['signal_bus'].set('')
        worker = asynctest.MagicMock()
        app.cfg.agent = unittest.mock.MagicMock()
        app.worker_start(worker)
        app.cfg.agent.assert_called()
        args, kwargs = app.cfg.agent.call_args
        self.assertEqual(kwargs['bus'], None)
        self.assertEqual(kwargs['broker'], None)

    @asynctest.mock.patch('pulsar.apps.data.create_store')
    async def test_app_with_redis_broker(self, *args, **kw):
        from microagent.tools.pulsar import MicroAgentApp, QueueBroker, PulsarRedisBroker
        app = MicroAgentApp()
        app.cfg.settings['queue_broker'] = QueueBroker()
        app.cfg.settings['queue_broker'].set('redis://127.0.0.1:6379/7')
        worker = asynctest.MagicMock()
        app.cfg.agent = unittest.mock.MagicMock()
        app.worker_start(worker)
        app.cfg.agent.assert_called()
        args, kwargs = app.cfg.agent.call_args
        self.assertIsInstance(kwargs['broker'], PulsarRedisBroker)

    @asynctest.mock.patch('pulsar.apps.data.create_store')
    async def test_app_with_amqp_broker(self, *args, **kw):
        from microagent.tools.pulsar import MicroAgentApp, QueueBroker
        from microagent.tools.amqp import AMQPBroker
        app = MicroAgentApp()
        app.cfg.settings['queue_broker'] = QueueBroker()
        app.cfg.settings['queue_broker'].set('amqp://fake')
        worker = asynctest.MagicMock()
        app.cfg.agent = unittest.mock.MagicMock()
        app.worker_start(worker)
        app.cfg.agent.assert_called()
        args, kwargs = app.cfg.agent.call_args
        self.assertIsInstance(kwargs['broker'], AMQPBroker)
