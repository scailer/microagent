# mypy: ignore-errors
import asyncio

from unittest.mock import AsyncMock, MagicMock, Mock

import aiokafka
import pytest

from microagent.queue import Consumer, Queue
from microagent.tools.kafka import KafkaBroker


@pytest.fixture()
def kafka_producer(monkeypatch):
    producer = MagicMock(
        start=AsyncMock(),
        send_and_wait=AsyncMock(),
        stop=AsyncMock(),
        _closed=True
    )
    monkeypatch.setattr(aiokafka, 'AIOKafkaProducer', Mock(return_value=producer))
    return producer


@pytest.fixture()
def kafka_consumer(monkeypatch):
    class KCMock:
        start = AsyncMock()
        stop = AsyncMock()

        async def __aiter__(self):
            yield MagicMock(value='{"a":1}')

    consumer = KCMock()
    monkeypatch.setattr(aiokafka, 'AIOKafkaConsumer', Mock(return_value=consumer))
    return consumer


async def test_broker_consume_ok(kafka_consumer):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(), queue=queue, timeout=1, options={})

    broker = KafkaBroker('kafka://localhost')
    await broker.bind_consumer(consumer)
    await asyncio.sleep(.01)

    kafka_consumer.start.assert_called()
    kafka_consumer.stop.assert_called()
    assert consumer.handler.call_args.kwargs['a'] == 1
    assert consumer.handler.call_args.kwargs['kafka']


async def test_broker_handler_fail_error(kafka_consumer):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=1, queue=queue, timeout=1, options={})

    broker = KafkaBroker('kafka://localhost')
    await broker._handle(consumer, {'a': 1})


async def test_broker_handler_fail_timeout(kafka_consumer):
    queue = Queue(name='test_queue')
    consumer = Consumer(agent=None, handler=AsyncMock(side_effect=asyncio.TimeoutError),
        queue=queue, timeout=1, options={})

    broker = KafkaBroker('kafka://localhost')
    await broker._handle(consumer, {'a': 1})


async def test_broker_send_ok(kafka_producer):
    queue = Queue(name='test_queue')
    broker = KafkaBroker('kafka://localhost')
    await broker.send(queue.name, '{}')

    broker.producer.start.assert_called()
    broker.producer.send_and_wait.assert_called_once_with(queue.name, b'{}')
    broker.producer.stop.assert_called()

    broker.producer._closed = False
    await broker.send(queue.name, '{}')
    broker.producer.start.assert_called()
