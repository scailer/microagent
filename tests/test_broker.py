# mypy: ignore-errors
import logging

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from microagent.broker import AbstractQueueBroker, Consumer, Queue
from microagent.queue import QueueNotFound, SerializingError


DSN = 'redis://localhost'


@dataclass(frozen=True)
class DTOTest:
    uuid: int
    code: str


class Handler(AsyncMock):
    def __name__(self):  # noqa PLW3201
        return 'Handler'


@pytest.fixture()
def test_queue():
    return Queue(name='test_queue')


@pytest.fixture()
def else_queue():
    return Queue(name='else_queue')


async def test_Queue_register_ok(test_queue, else_queue):
    assert Queue.get_all()['test_queue'] is test_queue
    assert Queue.get_all()['else_queue'] is else_queue
    assert Queue.get_all()['test_queue'] == test_queue
    assert Queue.get_all()['else_queue'] == else_queue
    assert test_queue.name == 'test_queue'
    assert not test_queue == 1


async def test_Queue_repr_ok(test_queue):
    assert 'Queue' in str(test_queue)


async def test_Queue_get_ok(test_queue):
    assert Queue.get('test_queue') is test_queue


async def test_Queue_get_fail_not_found():
    with pytest.raises(QueueNotFound):
        Queue.get('not_found')


async def test_Queue_serialize_ok(test_queue):
    assert test_queue.serialize({'a': 1}) == '{"a": 1}'


async def test_Queue_serialize_fail(test_queue):
    with pytest.raises(SerializingError):
        test_queue.serialize({'a': logging})


async def test_Queue_deserialize_ok(test_queue):
    assert test_queue.deserialize('{"a":1}') == {'a': 1}


async def test_Queue_deserialize_fail(test_queue):
    with pytest.raises(SerializingError):
        test_queue.deserialize('fail')


async def test_Consumer_ok(test_queue):
    consumer = Consumer(agent=None, handler=lambda: 1, queue=test_queue, timeout=60, options={})
    assert 'Consumer' in str(consumer)
    assert consumer.agent is None
    assert consumer.handler() == 1
    assert consumer.queue is test_queue
    assert consumer.timeout == 60


class Broker(AbstractQueueBroker):
    dsn: str
    uid: str

    async def send(self, channel: str, message: str) -> None:
        pass

    async def bind(self, channel: str) -> None:
        pass

    async def queue_length(self, name: str) -> None:
        pass


@pytest.fixture
def broker():
    broker = Broker(dsn=DSN)
    broker.log = MagicMock()
    broker.bind = AsyncMock()
    broker.send = AsyncMock()
    broker.queue_length = AsyncMock()
    return broker


async def test_Broker_init_ok(broker):
    assert broker.dsn == DSN
    assert 'Broker' in str(broker)

    await broker.send('channel', 'message')
    await broker.bind('channel')
    await broker.queue_length('channel')

    logger = logging.getLogger('name')
    broker = Broker(dsn=DSN, log=logger)
    assert broker.log is logger

    with pytest.raises(TypeError):
        Broker()  # noqa


async def test_Broker_bind_ok(broker, test_queue):
    consumer = Consumer(agent=None, handler=Handler(), queue=test_queue, timeout=60, options={})
    await broker.bind_consumer(consumer)
    broker.bind.assert_called_once_with(consumer.queue.name)


async def test_Broker_bind_fail(broker, test_queue):
    consumer = Consumer(agent=None, handler=Handler(), queue=test_queue, timeout=60, options={})
    broker._bindings[consumer.queue.name] = consumer
    await broker.bind_consumer(consumer)
    broker.bind.assert_not_called()
    broker.log.warning.assert_called()


async def test_Broker_send_ok(broker, test_queue):
    await broker.test_queue.send({'uid': 1})
    broker.send.assert_called_once_with('test_queue', '{"uid": 1}')


async def test_Broker_prepared_data_ok(broker, test_queue):
    consumer = Consumer(agent=None, handler=Handler(), queue=test_queue, timeout=60,
        dto_class=DTOTest, dto_name='obj', options={})
    data = broker.prepared_data(consumer, '{"uuid": 1, "code": "code"}')
    assert data == {'uuid': 1, 'code': 'code', 'obj': DTOTest(uuid=1, code='code')}


async def test_Broker_length_ok(broker, test_queue):
    await broker.test_queue.length()
    broker.queue_length.assert_called_once_with('test_queue')
