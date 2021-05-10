'''
The data-driven architecture is based on unidirectional message flows between agents.
Here we assume that messages are exchanged through an intermediary, not directly.

Here, an intermediary called Queue Broker implements the producer / consumer pattern.
The broker performs the functions of guaranteed and consistent transmission of messages
from the product to the consumer, many to one (or according to the broker's own logic).
The message has a free structure, fully defined in the user area.

Implementations:

* :ref:`aioredis <tools-aioredis>`
* :ref:`aioamqp <tools-amqp>`
* :ref:`kafka <tools-kafka>`


Using QueueBroker separately (sending only)

.. code-block:: python

    from microagent import load_queues
    from microagent.tools.aioredis import AIORedisBroker

    queues = load_queues('file://queues.json')

    broker = AIORedisBroker('redis://localhost/7')
    await broker.user_created.send({'user_id': 1})


Using with MicroAgent

.. code-block:: python

    from microagent import MicroAgent, load_queues
    from microagent.tools.aioredis import AIORedisSignalBus

    queues = load_queues('file://queues.json')

    class EmailAgent(MicroAgent):
        @consumer(queues.mailer)
        async def example_read_queue(self, **kwargs):
            await self.broker.email_sended.send({'user_id': 1})

    broker = AIORedisBroker('redis://localhost/7')
    email_agent = EmailAgent(broker=broker)
    await email_agent.start()
'''
import abc
import uuid
import logging

from typing import Dict, Optional
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
    '''
        Broker is an abstract interface with two basic methods - send and bind.

        `send`-method allows to write a message to the queue.

        `bind`-method allows to connect to queue for reading.

        All registered Queue are available in the broker object as attributes
        with the names specified in the declaration.

        .. code-block:: python

            Queue(name='user_created')

            broker = AIORedisBroker('redis://localhost/7')
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
        '''
            Write a raw message to queue.

            :param name: string, queue name
            :param message: string, serialized object
            :param \*\*kwargs: specific parameters for each broker implementation
        '''  # noqa: W605
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    async def bind(self, name: str) -> None:
        '''
            Start reading queue.

            :param name: string, queue name
        '''
        raise NotImplementedError  # pragma: no cover

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

    @abc.abstractmethod
    async def queue_length(self, name: str, **options) -> int:
        '''
            Get the current queue length.

            :param name: string, queue name
            :param \*\*options: specific parameters for each broker implementation
        '''  # noqa: W605
        return NotImplemented  # pragma: no cover
