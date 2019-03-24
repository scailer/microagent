import abc
import uuid
import logging
import asyncio

from typing import Optional
from .queue import Queue


class BoundQueue:
    __slots__ = ('broker', 'queue')

    def __init__(self, broker, queue):
        self.broker = broker
        self.queue = queue

    async def send(self, message, **options):
        await self.broker.send(self.queue.name, self.queue.serialize(message), **options)

    async def length(self):
        return await self.broker.queue_length(self.queue.name)


class AbstractQueueBroker(abc.ABC):
    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        self.uid = uuid.uuid4().hex
        self.dsn = dsn
        self.log = logger or logging.getLogger('microagent.queue')
        self._loop = asyncio.get_event_loop()
        self._bindings = {}

    def __getattr__(self, name: str):
        return BoundQueue(self, Queue.get(name))

    @abc.abstractmethod
    def send(self, name: str, message: str, **kwargs):
        return NotImplemented  # pragma: no cover

    @abc.abstractmethod
    def bind(self, name: str, handler):
        return NotImplemented  # pragma: no cover

    def bind_consumer(self, consumer):
        return self.bind(consumer.queue.name, consumer)

    @abc.abstractmethod
    def queue_length(self, name, **options):
        return NotImplemented  # pragma: no cover
