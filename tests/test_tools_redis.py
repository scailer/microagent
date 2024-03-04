# mypy: ignore-errors
import asyncio

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import redis.asyncio as redis

from microagent.queue import Consumer, Queue
from microagent.signal import Signal
from microagent.tools.redis import RedisBroker as RedisBrokerBase, RedisSignalBus


class RedisBroker(RedisBrokerBase):
    async def _wait(self, name: str) -> None:
        pass


@pytest.fixture()
def test_signal():
    return Signal(name='test_signal', providing_args=['uuid'])


async def test_bus_receive_ok(monkeypatch):
    class PubSub:
        PUBLISH_MESSAGE_TYPES = ('message', 'pmessage')
        psubscribe = AsyncMock()

        async def __aenter__(self):
            return MagicMock(PUBLISH_MESSAGE_TYPES=('message', 'pmessage'), listen=self.listen)

        async def __aexit__(self, exc_type, exc, traceback) -> None:  # noqa ANN001
            pass

        async def listen(self):
            yield {'type': 'pmessage', 'channel': 'pattern1', 'data': 'data'}
            raise redis.ConnectionError()

    conn = MagicMock(pubsub=PubSub, blpop=AsyncMock(return_value=(None, '')))
    monkeypatch.setattr(redis.Redis, 'from_url', Mock(return_value=conn))

    bus = RedisSignalBus('redis://localhost')
    bus.receiver = Mock()

    await bus.bind('pattern1')
    await asyncio.sleep(.02)

    assert 'pattern1' in [x[0][0] for x in PubSub.psubscribe.call_args_list]
    assert 'pattern1' in [x[0][0] for x in bus.receiver.call_args_list]


async def test_bus_send_ok(monkeypatch):
    conn = MagicMock(publish=AsyncMock(), blpop=AsyncMock(return_value=(None, '')))
    monkeypatch.setattr(redis.Redis, 'from_url', Mock(return_value=conn))

    bus = RedisSignalBus('redis://localhost')

    await bus.send('PUBSUB', '{}')

    conn.publish.assert_called_once_with('PUBSUB', '{}')

    await bus.send('PUBSUB', '{}')

    assert conn.publish.call_count == 2
    await asyncio.sleep(.1)


async def test_broker_consume_ok(monkeypatch):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    create_redis = MagicMock(blpop=AsyncMock(side_effect=[('ch', '{}'), None, Exception]))
    monkeypatch.setattr(redis, 'Redis', MagicMock(from_url=Mock(return_value=create_redis)))

    broker = RedisBroker('redis://localhost')
    broker.BIND_TIME = .01
    await broker.bind_consumer(consumer)
    await asyncio.sleep(.1)


async def test_broker_send_ok(monkeypatch):
    broker = RedisBroker('redis://localhost')
    broker.connection = MagicMock(rpush=AsyncMock(), blpop=AsyncMock(return_value=(None, '')))
    await broker.send('test_queue', '{}')

    broker.connection.rpush.assert_called_once_with('test_queue', '{}')

    await broker.send('test_queue', '{}')

    assert broker.connection.rpush.call_count == 2

    await asyncio.sleep(.1)


async def test_broker_queue_length_ok(monkeypatch):
    broker = RedisBroker('redis://localhost')
    broker.connection = MagicMock(llen=AsyncMock(), blpop=AsyncMock(return_value=(None, '')))
    await broker.queue_length('test_queue')

    broker.connection.llen.assert_called_once_with('test_queue')

    await broker.queue_length('test_queue')

    assert broker.connection.llen.call_count == 2


async def test_broker_handling_async_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    broker = RedisBroker('redis://localhost')
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once_with()


async def test_broker_handling_async_rollback_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(side_effect=Exception),
        queue=queue, timeout=1, options={})
    broker = RedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once()
    await asyncio.sleep(.01)
    broker.send.assert_called_once_with(queue.name, '{}')


async def test_broker_handling_async_rollback_timeout_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(side_effect=asyncio.TimeoutError),
        queue=queue, timeout=1, options={})
    broker = RedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once()
    await asyncio.sleep(.01)
    broker.send.assert_called_once_with(queue.name, '{}')


async def test_broker_rollback_many_attempts_ok():
    queue = Queue(name='test_queue')
    broker = RedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._rollbacks[str(hash(queue.name)) + str(hash('{}'))] = 4
    await broker.rollback(queue.name, '{}')
    broker.send.assert_not_called()
    await asyncio.sleep(.01)
