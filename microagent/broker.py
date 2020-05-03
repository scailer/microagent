import abc
import uuid
import logging
import asyncio

from typing import Dict, Optional, Protocol, Callable, runtime_checkable
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


@runtime_checkable
class AbstractQueueBroker(Protocol):
    uid: str
    dsn: str
    log: logging.Logger
    _bindings: Dict[str, Consumer]

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
    async def bind(self, name: str) -> None:
        return NotImplemented  # pragma: no cover

    async def bind_consumer(self, consumer: Consumer) -> None:
        if consumer.queue.name in self._bindings:
            self.log.warning('Handler to queue "%s" already binded. Ignoring', consumer)
            return

        self._bindings[consumer.queue.name] = consumer
        await self.bind(consumer.queue.name)

        self.log.debug('Bind %s to queue "%s"', consumer, consumer.queue.name)

    @abc.abstractmethod
    async def queue_length(self, name: str, **options) -> int:
        return NotImplemented  # pragma: no cover
