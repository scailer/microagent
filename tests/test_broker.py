import pytest
import asynctest
from microagent.broker import AbstractQueueBroker
from microagent import Queue

DSN = 'redis://localhost'


class Broker(AbstractQueueBroker):
    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass

    async def queue_length(self, name):
        pass


@pytest.fixture
def broker():
    broker = Broker(dsn=DSN)
    broker.bind = asynctest.CoroutineMock()
    broker.send = asynctest.CoroutineMock()
    broker.queue_length = asynctest.CoroutineMock()
    return broker


def test_init(broker):
    assert broker.dsn == DSN
    assert 'Broker' in str(broker)


async def test_bind(broker):
    test_queue = Queue(name='test_signal')
    consumer = asynctest.CoroutineMock(queue=test_queue)
    await broker.bind_consumer(consumer)
    broker.bind.assert_called_once()
    broker.bind.assert_called_with(test_queue.name, consumer)


async def test_send(broker):
    await broker.test_queue.send({'uid': 1})
    broker.send.assert_called_once()
    broker.send.assert_called_with('test_queue', '{"uid":1}')


async def test_length(broker):
    await broker.test_queue.length()
    broker.queue_length.assert_called_once()
    broker.queue_length.assert_called_with('test_queue')
