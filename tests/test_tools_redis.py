import asyncio
import pytest
import aioredis
from unittest.mock import MagicMock, AsyncMock, Mock
from microagent.signal import Signal
from microagent.queue import Queue, Consumer
from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker


@pytest.fixture()
def test_signal():
    return Signal(name='test_signal', providing_args=['uuid'])


async def test_bus_receive_ok(monkeypatch):
    async def mpsc_iter():
        yield '', (b'channel', b'{}')

    receiver = MagicMock(**{'iter': mpsc_iter, 'pattern': Mock(return_value='pattern')})
    create_redis = MagicMock(psubscribe=AsyncMock())
    monkeypatch.setattr(aioredis.pubsub, 'Receiver', lambda loop: receiver)
    monkeypatch.setattr(aioredis, 'create_redis', AsyncMock(return_value=create_redis))

    bus = AIORedisSignalBus('redis://localhost')
    bus.receiver = Mock()
    await asyncio.sleep(.01)

    assert bus.mpsc is receiver
    assert bus.pubsub is create_redis
    assert bus.transport is None
    bus.receiver.assert_called_once_with('channel', '{}')
    create_redis.psubscribe.assert_called_once_with('pattern')

    await bus.bind('pattern1')

    assert create_redis.psubscribe.call_count == 2


async def test_bus_send_ok(monkeypatch):
    create_redis = MagicMock(publish=AsyncMock())
    monkeypatch.setattr(aioredis.pubsub, 'Receiver', lambda loop: MagicMock())
    monkeypatch.setattr(AIORedisSignalBus, '_receiver', AsyncMock())
    monkeypatch.setattr(aioredis, 'create_redis', AsyncMock(return_value=create_redis))

    bus = AIORedisSignalBus('redis://localhost')
    await bus.send('PUBSUB', '{}')

    assert bus.transport is create_redis
    create_redis.publish.assert_called_once_with('PUBSUB', '{}')

    await bus.send('PUBSUB', '{}')

    assert create_redis.publish.call_count == 2


async def test_broker_consume_ok(monkeypatch):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    create_redis = MagicMock(blpop=AsyncMock(side_effect=[('ch', '{}'), None, Exception]))
    monkeypatch.setattr(aioredis, 'create_redis', AsyncMock(return_value=create_redis))

    broker = AIORedisBroker('redis://localhost')
    broker.BIND_TIME = .01
    await broker.bind_consumer(consumer)
    await asyncio.sleep(.02)


async def test_broker_send_ok(monkeypatch):
    create_redis = MagicMock(rpush=AsyncMock())
    monkeypatch.setattr(aioredis, 'create_redis', AsyncMock(return_value=create_redis))

    broker = AIORedisBroker('redis://localhost')
    await broker.send('test_queue', '{}')

    create_redis.rpush.assert_called_once_with('test_queue', '{}')

    await broker.send('test_queue', '{}')

    assert create_redis.rpush.call_count == 2


async def test_broker_queue_length_ok(monkeypatch):
    create_redis = MagicMock(llen=AsyncMock())
    monkeypatch.setattr(aioredis, 'create_redis', AsyncMock(return_value=create_redis))

    broker = AIORedisBroker('redis://localhost')
    await broker.queue_length('test_queue')

    create_redis.llen.assert_called_once_with('test_queue')

    await broker.queue_length('test_queue')

    assert create_redis.llen.call_count == 2


async def test_broker_handling_async_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    broker = AIORedisBroker('redis://localhost')
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once_with()


async def test_broker_handling_async_rollback_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(side_effect=Exception),
        queue=queue, timeout=1, options={})
    broker = AIORedisBroker('redis://localhost')
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
    broker = AIORedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once()
    await asyncio.sleep(.01)
    broker.send.assert_called_once_with(queue.name, '{}')


async def test_broker_rollback_many_attempts_ok():
    queue = Queue(name='test_queue')
    broker = AIORedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._rollbacks[str(hash(queue.name)) + str(hash('{}'))] = 4
    await broker.rollback(queue.name, '{}')
    broker.send.assert_not_called()


async def test_broker_handling_sync_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=Mock(), queue=queue, timeout=1, options={})
    broker = AIORedisBroker('redis://localhost')
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once_with()


async def test_broker_handling_sync_rollback_ok():
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=Mock(side_effect=Exception),
        queue=queue, timeout=1, options={})
    broker = AIORedisBroker('redis://localhost')
    broker.send = AsyncMock()
    broker._bindings[queue.name] = consumer
    await broker._handler(queue.name, '{}')
    consumer.handler.assert_called_once()
    await asyncio.sleep(.01)
    broker.send.assert_called_once_with(queue.name, '{}')
