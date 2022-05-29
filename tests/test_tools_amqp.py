# mypy: ignore-errors
import asyncio
import pytest
import aioamqp
from unittest.mock import MagicMock, AsyncMock, Mock
from microagent.queue import Queue, Consumer
from microagent.tools.amqp import AMQPBroker


@pytest.fixture()
def amqp_connection(monkeypatch):
    channel = MagicMock(
        basic_consume=AsyncMock(),
        queue_declare=AsyncMock(),
        basic_publish=AsyncMock(),
        basic_client_ack=AsyncMock(),
        close=AsyncMock()
    )
    connection = MagicMock(channel=AsyncMock(return_value=channel), channels={})
    monkeypatch.setattr(aioamqp, 'connect', AsyncMock(return_value=(None, connection)))
    return channel, connection


async def test_broker_consume_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    await broker.bind_consumer(consumer)

    connection.channel.assert_called()
    channel.basic_consume.assert_called()


async def test_broker_consume_new_channel_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection
    channel.basic_consume.side_effect = [aioamqp.ChannelClosed(code=404), None]

    broker = AMQPBroker('amqp://localhost')
    await broker.bind_consumer(consumer)

    connection.channel.assert_called()
    channel.queue_declare.assert_called_once_with(queue.name)
    assert channel.basic_consume.call_count == 2


async def test_broker_consume_on_error_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    broker.rebind = AsyncMock()
    await broker.bind_consumer(consumer)
    broker._on_amqp_error(queue.name, Exception())
    broker._on_amqp_error('no_so_queue', Exception())
    broker.rebind.assert_called_once_with(queue.name)


async def test_broker_consume_fail(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection
    channel.basic_consume.side_effect = aioamqp.ChannelClosed(code=500)

    broker = AMQPBroker('amqp://localhost')

    with pytest.raises(aioamqp.ChannelClosed):
        await broker.bind_consumer(consumer)


async def test_broker_rebind_ok():
    broker = AMQPBroker('amqp://localhost')
    broker._bind_attempts['test_queue'] = 0
    broker.bind = AsyncMock()
    assert await broker.rebind('test_queue')
    broker.bind.assert_called_once_with('test_queue')


async def test_broker_rebind_ok_twice():
    broker = AMQPBroker('amqp://localhost')
    broker._bind_attempts['test_queue'] = 0
    broker._bindings['test_queue'] = AsyncMock()

    async def foo(*args, **kwargs):
        await asyncio.sleep(.001)

    broker.bind = AsyncMock(side_effect=foo)
    broker._on_amqp_error('test_queue', aioamqp.ChannelClosed())
    await asyncio.sleep(.0005)
    broker._on_amqp_error('test_queue', aioamqp.ChannelClosed())
    await asyncio.sleep(.001)

    broker.bind.assert_called_once()


async def test_broker_rebind_fail_limit():
    broker = AMQPBroker('amqp://localhost')
    broker.bind = AsyncMock()
    broker._bind_attempts['test_queue'] = 4
    try:
        assert await broker.rebind('test_queue') is False
    except AttributeError:  # for travis
        pass
    broker.bind.assert_not_called()


async def test_broker_rebind_fail_oserror():
    broker = AMQPBroker('amqp://localhost')
    broker.bind = AsyncMock(side_effect=OSError())
    broker._bind_attempts['test_queue'] = 0
    assert not await broker.rebind('test_queue')


async def test_broker_rebind_fail_close_conn():
    broker = AMQPBroker('amqp://localhost')
    broker.bind = AsyncMock(side_effect=aioamqp.AmqpClosedConnection())
    broker._bind_attempts['test_queue'] = 0
    assert not await broker.rebind('test_queue')


async def test_broker_rebind_fail_close_channel():
    broker = AMQPBroker('amqp://localhost')
    broker.bind = AsyncMock(side_effect=aioamqp.ChannelClosed())
    broker._bind_attempts['test_queue'] = 0
    assert not await broker.rebind('test_queue')


async def test_broker_send_ok(amqp_connection):
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    await broker.send('test_queue', '{}')

    channel.basic_publish.assert_called_once_with(
        b'{}', routing_key='test_queue', exchange_name='')
    channel.close.assert_called()
    assert broker.protocol is connection


async def test_broker_declare_queue_ok(amqp_connection):
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    await broker.declare_queue('test_queue')

    channel.queue_declare.assert_called_once_with('test_queue')
    channel.close.assert_called()
    assert broker.protocol is connection


async def test_broker_queue_length_ok(amqp_connection):
    channel, connection = amqp_connection
    channel.queue_declare.return_value = {'message_count': '7'}

    broker = AMQPBroker('amqp://localhost')
    assert await broker.queue_length('test_queue') == 7

    channel.queue_declare.assert_called_once_with('test_queue')
    channel.close.assert_called()
    assert broker.protocol is connection


async def test_broker_channel_ok(amqp_connection):
    channel, connection = amqp_connection
    channel.basic_consume.side_effect = aioamqp.ChannelClosed(code=500)

    broker = AMQPBroker('amqp://localhost')
    assert await broker.get_channel()
    connection.channel.assert_called()
    conn_id = id(broker.protocol)

    assert await broker.get_channel()
    assert connection.channel.call_count == 2
    assert id(broker.protocol) == conn_id
    assert broker.channels == {}

    connection.channel.side_effect = aioamqp.AmqpClosedConnection

    with pytest.raises(aioamqp.AmqpClosedConnection):
        await broker.get_channel()

    assert not broker.protocol
    assert broker.channels == {}


async def test_broker_wrapper_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, '{"a":1}', MagicMock(delivery_tag=1), MagicMock())

    consumer.handler.assert_called_once()
    assert consumer.handler.call_args.kwargs['a'] == 1
    assert consumer.handler.call_args.kwargs['amqp']
    channel.basic_client_ack.assert_called_once_with(delivery_tag=1)


async def test_broker_wrapper_sync_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=Mock(), queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, '{"a":1}', MagicMock(delivery_tag=1), MagicMock())

    consumer.handler.assert_called_once()
    assert consumer.handler.call_args.kwargs['a'] == 1
    assert consumer.handler.call_args.kwargs['amqp']
    channel.basic_client_ack.assert_called_once_with(delivery_tag=1)


async def test_broker_wrapper_noack_ok(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue,
        timeout=1, options={'autoack': False})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, '{"a":1}', MagicMock(delivery_tag=1), MagicMock())

    consumer.handler.assert_called_once()
    assert consumer.handler.call_args.kwargs['a'] == 1
    assert consumer.handler.call_args.kwargs['amqp']
    channel.basic_client_ack.assert_not_called()


async def test_broker_wrapper_fail_type(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=1, queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, '{"a":1}', MagicMock(delivery_tag=1), MagicMock())

    channel.basic_client_ack.assert_not_called()


async def test_broker_wrapper_fail_empty_body(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=1, queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, 'null', MagicMock(delivery_tag=1), MagicMock())

    channel.basic_client_ack.assert_not_called()


async def test_broker_wrapper_fail_timeout(amqp_connection):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(side_effect=asyncio.TimeoutError),
        queue=queue, timeout=1, options={})
    channel, connection = amqp_connection

    broker = AMQPBroker('amqp://localhost')
    wrapper = broker._amqp_wrapper(consumer)
    await wrapper(channel, '{"a":1}', MagicMock(delivery_tag=1), MagicMock())

    channel.basic_client_ack.assert_not_called()


async def test_broker_putout_ok():
    broker = AMQPBroker('amqp://localhost')
    broker.get_channel = AsyncMock(return_value=MagicMock())
    amqp = MagicMock(
        channel=MagicMock(basic_client_ack=AsyncMock(), is_open=True),
        envelope=MagicMock(delivery_tag=1)
    )

    await broker.putout(amqp)

    amqp.channel.basic_client_ack.assert_called_once_with(delivery_tag=1)


async def test_broker_putout_ok_closed():
    broker = AMQPBroker('amqp://localhost')
    channel = MagicMock(basic_client_ack=AsyncMock())
    broker.get_channel = AsyncMock(return_value=channel)
    amqp = MagicMock(
        channel=MagicMock(is_open=False),
        envelope=MagicMock(delivery_tag=1)
    )

    await broker.putout(amqp)

    broker.get_channel.assert_called()
    channel.basic_client_ack.assert_called_once_with(delivery_tag=1)
