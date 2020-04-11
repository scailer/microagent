import abc
import uuid
import logging
import asyncio

from typing import Optional
from .queue import Queue, Consumer


class BoundQueue:
    __slots__ = ('broker', 'queue')

    broker: 'AbstractQueueBroker'
    queue: Queue

    def __init__(self, broker: 'AbstractQueueBroker', queue: Queue):
        self.broker = broker
        self.queue = queue

    async def send(self, message: dict, **options) -> None:
        await self.broker.send(self.queue.name, self.queue.serialize(message), **options)

    async def length(self) -> int:
        return await self.broker.queue_length(self.queue.name)


class AbstractQueueBroker(abc.ABC):
    uid: str
    dsn: str
    log: logging.Logger
    _bindings: dict

    def __new__(cls, dsn, **kwargs) -> 'AbstractQueueBroker':
        broker = super(AbstractQueueBroker, cls).__new__(cls)

        broker.uid = uuid.uuid4().hex
        broker.log = logging.getLogger('microagent.broker')
        broker._bindings = {}

        return broker

    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        self.dsn = dsn

        if logger:
            self.log = logger

    def __getattr__(self, name: str) -> BoundQueue:
        return BoundQueue(self, Queue.get(name))

    @abc.abstractmethod
    async def send(self, name: str, message: str, **kwargs) -> None:
        return NotImplemented  # pragma: no cover

    @abc.abstractmethod
    async def bind(self, name: str, handler) -> None:
        return NotImplemented  # pragma: no cover

    async def bind_consumer(self, consumer: Consumer) -> None:
        await self.bind(consumer.queue.name, consumer)

    @abc.abstractmethod
    async def queue_length(self, name: str, **options) -> int:
        return NotImplemented  # pragma: no cover
