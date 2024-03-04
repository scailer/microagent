# mypy: ignore-errors
import asyncio

from unittest.mock import AsyncMock, MagicMock, Mock

import aiormq
import pytest

from microagent.queue import Consumer, Queue
from microagent.tools import amqp
from microagent.tools.amqp import AMQPBroker, Connection, ManagedConnection, ReConnection


@pytest.fixture()
def amqp_connection(monkeypatch):
    channel = MagicMock(
        basic_consume=AsyncMock(),
        queue_declare=AsyncMock(),
        basic_publish=AsyncMock(),
        basic_nack=AsyncMock(),
        basic_ack=AsyncMock(),
        close=AsyncMock()
    )
    connection = MagicMock(channel=AsyncMock(return_value=channel), channels={})
    monkeypatch.setattr(aiormq, 'Connection', MagicMock(channel=AsyncMock(return_value=channel)))
    return channel, connection


@pytest.fixture()
def channel(monkeypatch):
    channel = MagicMock(
        basic_consume=AsyncMock(),
        queue_declare=AsyncMock(),
        basic_publish=AsyncMock(),
        basic_nack=AsyncMock(),
        basic_ack=AsyncMock(),
        close=AsyncMock()
    )
    monkeypatch.setattr(Connection, 'connect', AsyncMock())
    monkeypatch.setattr(Connection, 'channel', AsyncMock(return_value=channel))
    return channel


async def test_ReConnection_ok(monkeypatch):
    monkeypatch.setattr(Connection, '_on_close', AsyncMock())
    handler = AsyncMock()
    conn = ReConnection(handler, 'amqp://localhost')
    await conn._on_close()
    handler.assert_called()


async def test_broker_send_ok(channel):
    queue = Queue(name='test_queue')
    broker = AMQPBroker('amqp://localhost')
    await broker.send(queue.name, '{"a":1}')

    channel.basic_publish.assert_called_once_with(
        b'{"a":1}', routing_key='test_queue', exchange='', properties=None)


async def test_broker_send_ok_fail(monkeypatch, channel):
    monkeypatch.setattr(Connection, 'channel', AsyncMock(side_effect=Exception))

    queue = Queue(name='test_queue')
    broker = AMQPBroker('amqp://localhost')
    conn_id = id(broker.connection)

    with pytest.raises(Exception):  # noqa B017
        await broker.send(queue.name, '{"a":1}')

    channel.basic_publish.assert_not_called()
    assert conn_id != id(broker.connection)


async def test_broker_bind_ok(channel):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})

    broker = AMQPBroker('amqp://localhost')
    await broker.bind_consumer(consumer)

    channel.basic_consume.assert_called()


async def test_broker_rebind_ok(monkeypatch, channel):
    conn = ManagedConnection('amqp://localhost', queue_name='test_queue', handler=AsyncMock())

    assert await conn.rebind()


async def test_broker_rebind_ok_already(monkeypatch, channel):
    conn = ManagedConnection('amqp://localhost', queue_name='test_queue', handler=AsyncMock())
    conn.bind_running = True

    assert not await conn.rebind()


async def test_broker_rebind_ok_attempts(monkeypatch, channel):
    conn = ManagedConnection('amqp://localhost', queue_name='test_queue', handler=AsyncMock())
    conn.bind_attempts = 4

    assert not await conn.rebind()


async def test_broker_rebind_ok_many(monkeypatch, channel):
    channel.basic_consume = AsyncMock(side_effect=aiormq.exceptions.ConnectionChannelError)
    conn = ManagedConnection('amqp://localhost', queue_name='test_queue', handler=AsyncMock())
    monkeypatch.setattr(amqp, 'REBIND_BASE_DELAY', 0)
    monkeypatch.setattr(amqp, 'REBIND_ATTEMPTS', 1)

    assert not await conn.rebind()
    assert not await conn.rebind()

    await asyncio.sleep(.1)


async def test_broker_declare_queue_ok(monkeypatch, channel):
    monkeypatch.setattr(Connection, 'connect', AsyncMock())
    monkeypatch.setattr(Connection, 'channel', AsyncMock(return_value=channel))

    queue = Queue(name='test_queue')
    broker = AMQPBroker('amqp://localhost')
    await broker.declare_queue(queue.name)

    channel.queue_declare.assert_called_once_with('test_queue')


async def test_broker_queue_length_ok(monkeypatch, channel):
    monkeypatch.setattr(Connection, 'connect', AsyncMock())
    monkeypatch.setattr(Connection, 'channel', AsyncMock(return_value=channel))

    queue = Queue(name='test_queue')
    broker = AMQPBroker('amqp://localhost')
    await broker.queue_length(queue.name)

    channel.queue_declare.assert_called_once_with('test_queue')


async def test_broker_putout_ok(monkeypatch, channel):
    monkeypatch.setattr(Connection, 'connect', AsyncMock())
    monkeypatch.setattr(Connection, 'channel', AsyncMock(return_value=channel))

    broker = AMQPBroker('amqp://localhost')
    await broker.putout(MagicMock(delivery_tag='123', channel=channel))
    await broker.putout(MagicMock(delivery_tag=None, channel=channel))

    channel.basic_ack.assert_called_once()


async def test_broker_wrapper_ok(channel):
    handler = AsyncMock()
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=handler, queue=queue, timeout=1, options={})
    broker = AMQPBroker('amqp://localhost')
    message = MagicMock(body=b'{"a":1}', delivery_tag='123', channel=channel)

    await broker._amqp_wrapper(consumer)(message)

    channel.basic_ack.assert_called()
    handler.assert_called()


async def test_broker_wrapper_ok_nodata(channel):
    handler = AsyncMock()
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=handler, queue=queue, timeout=1, options={})
    broker = AMQPBroker('amqp://localhost')
    message = MagicMock(body=b'{}', delivery_tag='123', channel=channel)

    await broker._amqp_wrapper(consumer)(message)

    channel.basic_ack.assert_not_called()
    handler.assert_not_called()


async def test_broker_wrapper_ok_type_err(channel):
    handler = Mock(side_effect=TypeError)
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=handler, queue=queue, timeout=1, options={})
    broker = AMQPBroker('amqp://localhost')
    message = MagicMock(body=b'{"a":1}', delivery_tag='123', channel=channel)

    await broker._amqp_wrapper(consumer)(message)

    channel.basic_ack.assert_not_called()
    handler.assert_called()


async def test_broker_wrapper_ok_timeout(channel):
    handler = AsyncMock(side_effect=asyncio.TimeoutError)
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=handler, queue=queue, timeout=1, options={})
    broker = AMQPBroker('amqp://localhost')
    message = MagicMock(body=b'{"a":1}', delivery_tag='123', channel=channel)

    await broker._amqp_wrapper(consumer)(message)

    channel.basic_ack.assert_not_called()
    handler.assert_called()


async def test_broker_wrapper_ok_not_autoack(channel):
    handler = AsyncMock()
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=handler, queue=queue,
        timeout=1, options={'autoack': False})
    broker = AMQPBroker('amqp://localhost')
    message = MagicMock(body=b'{"a":1}', delivery_tag='123', channel=channel)

    await broker._amqp_wrapper(consumer)(message)

    channel.basic_ack.assert_not_called()
    handler.assert_called()
