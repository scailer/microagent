import abc
import uuid
import logging
import asyncio

from typing import Optional
from .queue import Queue, Consumer


class BoundQueue:
    __slots__ = ('broker', 'queue')

    def __init__(self, broker: 'microagent.broker.AbstractQueueBroker', queue: Queue):
        self.broker = broker  # type: microagent.broker.AbstractQueueBroker
        self.queue = queue  # type: Queue

    async def send(self, message: dict, **options) -> None:
        await self.broker.send(self.queue.name, self.queue.serialize(message), **options)

    async def length(self) -> int:
        return await self.broker.queue_length(self.queue.name)


class AbstractQueueBroker(abc.ABC):
    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        self.uid = uuid.uuid4().hex  # type: str
        self.dsn = dsn  # type: str
        self.log = logger or logging.getLogger('microagent.queue')  # type: logging.Logger
        self._bindings = {}

    def __getattr__(self, name: str) -> BoundQueue:
        return BoundQueue(self, Queue.get(name))

    @abc.abstractmethod
    def send(self, name: str, message: str, **kwargs) -> None:
        return NotImplemented  # pragma: no cover

    @abc.abstractmethod
    def bind(self, name: str, handler) -> None:
        return NotImplemented  # pragma: no cover

    async def bind_consumer(self, consumer: Consumer) -> None:
        await self.bind(consumer.queue.name, consumer)

    @abc.abstractmethod
    def queue_length(self, name: str, **options) -> int:
        return NotImplemented  # pragma: no cover
