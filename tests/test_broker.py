import asynctest
from microagent.broker import AbstractQueueBroker
from microagent import Queue


class Broker(AbstractQueueBroker):
    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass

    async def declare_queue(self, name):
        pass

    async def queue_length(self, name):
        pass


class TestBus(asynctest.TestCase):
    def setUp(self):
        self.dsn = 'redis://localhost'
        self.broker = Broker(dsn=self.dsn)
        self.broker.bind = asynctest.CoroutineMock()
        self.broker.send = asynctest.CoroutineMock()
        self.broker.declare_queue = asynctest.CoroutineMock()
        self.broker.queue_length = asynctest.CoroutineMock()

    def test_init(self):
        self.assertEqual(self.broker.dsn, self.dsn)
        self.assertIn('Broker', str(self.broker))

    async def test_bind(self):
        test_queue = Queue(name='test_signal')
        consumer = asynctest.CoroutineMock(queue=test_queue)
        await self.broker.bind_consumer(consumer)
        self.broker.bind.assert_called_once()
        self.broker.bind.assert_called_with(test_queue.name, consumer)

    async def test_send(self):
        await self.broker.test_queue.send({'uid': 1})
        self.broker.send.assert_called_once()
        self.broker.send.assert_called_with('test_queue', '{"uid":1}')

    async def test_declare(self):
        await self.broker.test_queue.declare()
        self.broker.declare_queue.assert_called_once()
        self.broker.declare_queue.assert_called_with('test_queue')

    async def test_length(self):
        await self.broker.test_queue.length()
        self.broker.queue_length.assert_called_once()
        self.broker.queue_length.assert_called_with('test_queue')
