'''
The data-driven architecture is based on unidirectional message flows between agents.
Here we assume that messages are exchanged through an intermediary, not directly.

Here, an intermediary called Queue Broker implements the producer / consumer pattern.
The broker performs the functions of guaranteed and consistent transmission of messages
from the product to the consumer, many to one (or according to the broker's own logic).
The message has a free structure, fully defined in the user area.

Implementations:

* :ref:`redis <tools-redis>`
* :ref:`aioamqp <tools-amqp>`
* :ref:`kafka <tools-kafka>`


Using QueueBroker separately (sending only)

.. code-block:: python

    from microagent import load_queues
    from microagent.tools.redis import RedisBroker

    queues = load_queues('file://queues.json')

    broker = RedisBroker('redis://localhost/7')
    await broker.user_created.send({'user_id': 1})


Using with MicroAgent

.. code-block:: python

    from microagent import MicroAgent, load_queues
    from microagent.tools.redis import RedisSignalBus

    queues = load_queues('file://queues.json')

    class EmailAgent(MicroAgent):
        @consumer(queues.mailer)
        async def example_read_queue(self, **kwargs):
            await self.broker.email_sended.send({'user_id': 1})

    broker = RedisBroker('redis://localhost/7')
    email_agent = EmailAgent(broker=broker)
    await email_agent.start()
'''
import logging
import uuid

from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any

from .abc import BrokerProtocol, QueueProtocol
from .queue import Consumer, Queue


@dataclass(slots=True)
class AbstractQueueBroker(BrokerProtocol):
    '''
        Broker is an abstract interface with two basic methods - send and bind.

        `send`-method allows to write a message to the queue.

        `bind`-method allows to connect to queue for reading.

        All registered Queue are available in the broker object as attributes
        with the names specified in the declaration.

        .. code-block:: python

            Queue(name='user_created')

            broker = RedisBroker('redis://localhost/7')
            await broker.user_created.send({'user_id': 1})

        .. attribute:: dsn

            Broker has only one required parameter - dsn-string (data source name),
            which provide details for establishing connection to mediator-service.

        .. attribute:: log

            Provided or defaul logger

        .. attribute:: uid

            UUID, id of broker instance (for debugging)

        .. attribute:: _bindings

            Dict of all bound consumers
    '''

    dsn: str
    uid: str = field(default_factory=lambda: uuid.uuid4().hex)
    log: logging.Logger = logging.getLogger('microagent.broker')

    _bindings: dict[str, Consumer] = field(default_factory=dict)

    def __getattr__(self, name: str) -> 'BoundQueue':
        return BoundQueue(self, Queue.get(name))

    @abstractmethod
    async def send(self, name: str, message: str, **kwargs: Any) -> None:
        '''
            Write a raw message to queue.

            :param name: string, queue name
            :param message: string, serialized object
            :param \*\*kwargs: specific parameters for each broker implementation
        '''  # noqa: W605
        ...

    @abstractmethod
    async def bind(self, name: str) -> None:
        '''
            Start reading queue.

            :param name: string, queue name
        '''
        ...

    async def bind_consumer(self, consumer: Consumer) -> None:
        '''
            Bind bounded to agent consumer to current broker.
        '''

        if consumer.queue.name in self._bindings:
            self.log.warning('Handler to queue "%s" already binded. Ignoring', consumer)
            return

        self._bindings[consumer.queue.name] = consumer
        await self.bind(consumer.queue.name)

        self.log.debug('Bind %s to queue "%s"', consumer, consumer.queue.name)

    @staticmethod
    def prepared_data(consumer: Consumer, raw_data: str | bytes) -> dict:
        data = consumer.queue.deserialize(raw_data)
        if consumer.dto_class:
            data[consumer.dto_name or 'dto'] = consumer.dto_class(**data)
        return data

    @abstractmethod
    async def queue_length(self, name: str, **options: Any) -> int:
        '''
            Get the current queue length.

            :param name: string, queue name
            :param \*\*options: specific parameters for each broker implementation
        '''  # noqa: W605
        ...


@dataclass(slots=True, frozen=True)
class BoundQueue(QueueProtocol):
    broker: 'AbstractQueueBroker'
    queue: Queue

    async def send(self, message: dict, **options: Any) -> None:
        await self.broker.send(self.queue.name, self.queue.serialize(message), **options)

    async def length(self) -> int:
        return await self.broker.queue_length(self.queue.name)
