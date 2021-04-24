'''
MicroAgent is a **container** for **signal receivers**, **message consumers**
and **periodic tasks**. The MicroAgent links the abstract declarations of receivers
and consumers with the provided bus and broker. The MicroAgent initiates
periodic tasks and starts the servers.

The microagent can be launched using the supplied launcher.
Or it can be used as a stand-alone entity running in python-shell or a custom script.
It may be useful for testing, exploring, debugging and launching in specific purposes.


Agent declaration.

.. code-block:: python

    from microagent import MicroAgent, receiver, consumer, periodic, cron, on

    class Agent(MicroAgent):

        @on('pre_start')
        async def setup(self):
            pass

        @periodic(period=5)
        async def periodic_handler(self):
            pass

        @receiver(signals.send_mail)
        async def send_mail_handler(self, **kwargs):
            pass

        @consumer(queues.mailer)
        async def mail_handler(self, **kwargs):
            pass


Agent initiation.

.. code-block:: python

    import logging
    from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker

    # Initialize bus, broker and logger
    bus = AIORedisSignalBus('redis://localhost/7')
    broker = AIORedisBroker('redis://localhost/7')
    log = logging.getLogger('my_log')
    settings = {'secret': 'my_secret'}

    # Initialize MicroAgent, all arguments optional
    agent = Agent(bus=bus, broker=broker, logger=logger, settings=settings)


Manual launching.

.. code-block:: python

    await user_agent.start()  # freezed here if sub-servers running

    while True:
        await asyncio.sleep(60)


Using MicroAgent resources.

.. code-block:: python

    class Agent(MicroAgent):

        async def setup(self):
            self.log.info('Setup called!')  # write log
            await self.bus.my_sugnal.send(sender='agent', param=1)  # use bus
            await self.broker.my_queue.send({'text': 'Hello world!'})  # use broker
            secret = self.settings['secret']  # user settings
            print(self.info())  # serializable dict of agent structure
'''

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Iterable, Callable, Union, Dict, Tuple
from datetime import datetime, timedelta

from .signal import Signal, Receiver
from .queue import Queue, Consumer
from .bus import AbstractSignalBus
from .broker import AbstractQueueBroker
from .hooks import Hooks, Hook
from .periodic_task import PeriodicTask, CRONTask, CRON

HandlerTypes = Union[Receiver, Consumer, PeriodicTask, CRONTask, Hook]


class MicroAgent:
    '''
        MicroAgent is a **container** for **signal receivers**,
        **message consumers** and **periodic tasks**.

        The magic of declarations binding to an agent-object is implemented in *__new__*.
        Attaching the bus, broker and logger is implemented in *__init__*.
        Subscribing and initiating periodic tasks and servers is implemented in *start* method.

        To create specialized MicroAgent classes, you can override *__init__*,
        which is safe for the constructor logic. But it is usually sufficient to use
        *@on('pre_start')* decorator for sync or async methods for initializing
        resources and etc.

        :param bus: signal bus, object of subclass :class:`AbstractSignalBus`,
            required for receive or send the signals
        :param broker:  queue broker, object of subclass :class:`AbstractQueueBroker`,
            required for consume or send the messages
        :param logger: prepared :class:`logging.Logger`,
            or use default logger if not provided
        :param settings: dict of user settings storing in object

        .. attribute:: log

            Prepared python logger::

                self.log.info('Hellow world')

        .. attribute:: bus

            Signal bus, provided on initializing::

                await self.bus.send_mail.send('agent', user_id=1)

        .. attribute:: broker

            Queue broker, provided on initializing::

                await self.broker.mailer.send({'text': 'Hello world'})

        .. attribute:: settings

            Dict, user settings, provided on initializing, or empty.
    '''

    log: logging.Logger
    bus: Optional[AbstractSignalBus]
    broker: Optional[AbstractQueueBroker]
    settings: Dict

    periodic_tasks: Iterable[PeriodicTask]
    cron_tasks: Iterable[CRONTask]
    receivers: Iterable[Receiver]
    consumers: Iterable[Consumer]
    hook: Hooks

    def __new__(cls, **kwargs) -> 'MicroAgent':
        agent = super(MicroAgent, cls).__new__(cls)

        agent.periodic_tasks = PeriodicHandler.bound(agent)
        agent.cron_tasks = CRONHandler.bound(agent)
        agent.receivers = ReceiverHandler.bound(agent)
        agent.consumers = ConsumerHandler.bound(agent)
        agent.hook = Hooks(HookHandler.bound(agent))
        agent.log = logging.getLogger('microagent')
        agent.settings = {}

        if agent.receivers:
            bus = kwargs.get('bus')
            assert bus, 'Bus required'
            assert isinstance(bus, AbstractSignalBus), \
                f'Bus must be AbstractSignalBus instance or None instead {bus}'

        if agent.consumers:
            broker = kwargs.get('broker')
            assert broker, 'Broker required'
            assert isinstance(broker, AbstractQueueBroker), \
                f'Broker must be AbstractQueueBroker instance or None instead {broker}'

        return agent

    def __init__(self,
                bus: AbstractSignalBus = None,
                broker: AbstractQueueBroker = None,
                logger: logging.Logger = None,
                settings: dict = None
            ) -> None:

        self.bus = bus
        self.broker = broker
        self.settings = settings or {}

        if logger:
            self.log = logger

        self.log.debug('%s initialized', self)

    def __repr__(self) -> str:
        return f'<MicroAgent {self.__class__.__name__}>'

    async def start(self,
                enable_periodic_tasks: Optional[bool] = True,
                enable_receiving_signals: Optional[bool] = True,
                enable_consuming_messages: Optional[bool] = True,
                enable_servers_running: Optional[bool] = True
            ) -> None:
        '''
            Starting MicroAgent to receive signals, consume messages
            and initiate periodic running.

            :param enable_periodic_tasks: default enabled
            :param enable_consuming_messages: default enabled
            :param enable_receiving_signals: default enabled
            :param enable_servers_running: default enabled
        '''

        await self.hook.pre_start()

        if enable_receiving_signals:
            await self.bind_receivers(self.receivers)

        if enable_consuming_messages:
            await self.bind_consumers(self.consumers)

        if enable_periodic_tasks:
            self.run_periodic_tasks(self.periodic_tasks, self.cron_tasks)

        await self.hook.post_start()

        if enable_servers_running:
            await self.run_servers(self.hook.servers)

    async def stop(self) -> None:
        await self.hook.pre_stop()

    async def run_servers(self, servers: Iterable[Callable]) -> None:
        await asyncio.gather(*[server() for server in servers])

    def run_periodic_tasks(self, periodic_tasks: Iterable[PeriodicTask],
            cron_tasks: Iterable[CRONTask]) -> None:

        for task in [*periodic_tasks, *cron_tasks]:
            start_after = getattr(task, 'start_after', None) or 0  # type: Union[int, float]

            if start_after > 100:
                start_at = datetime.now() + timedelta(seconds=start_after)  # type: datetime
                self.log.debug('Set %s at %s', task, f'{start_at:%H:%M:%S}')
            else:
                self.log.debug('Set %s after %d sec', task, start_after)

            task.start(start_after)

    async def bind_receivers(self, receivers: Iterable[Receiver]) -> None:
        ''' Bind signal receivers to bus subscribers '''
        for receiver in receivers:
            await self.bus.bind_receiver(receiver)

    async def bind_consumers(self, consumers: Iterable[Consumer]) -> None:
        ''' Bind message consumers to queues '''
        for consumer in consumers:
            await self.broker.bind_consumer(consumer)

    def info(self) -> dict:
        '''
            Information about MicroAgent in json-serializable dict
        '''
        return {
            'name': self.__class__.__name__,
            'bus': str(self.bus) if self.bus else None,
            'broker': str(self.broker) if self.broker else None,
            'periodic': [
                {
                    'name': task.handler.__name__,
                    'period': task.period,
                }
                for task in self.periodic_tasks
            ],
            'cron': [
                {
                    'name': task.handler.__name__,
                    'cron': str(task.cron),
                }
                for task in self.cron_tasks
            ],
            'receivers': [
                {
                    'name': receiver.handler.__name__,
                    'signal': receiver.signal.name,
                }
                for receiver in self.receivers
            ],
            'consumers': [
                {
                    'name': consumer.handler.__name__,
                    'queue': consumer.queue.name,
                }
                for consumer in self.consumers
            ],
        }


class UnboundHandler:
    handler: Callable
    target_class: type
    _register: Dict

    def __post_init__(self):
        key = self.handler.__module__, *self.handler.__qualname__.split('.')
        self._register[key] = self

    @classmethod
    def bound(cls, agent: MicroAgent):
        classes, result = [], []

        for _cls in agent.__class__.__mro__:
            if _cls.__module__ != 'builtins' and _cls.__name__ != 'MicroAgent':
                classes.append((_cls.__module__, *_cls.__qualname__.split('.')))

        for lookup_key, unbound in cls._register.items():
            args = unbound.__dict__
            args.pop('handler', None)

            if tuple(lookup_key[:-1]) in classes:
                result.append(cls.target_class(
                    agent=agent,
                    handler=getattr(agent, lookup_key[-1].split(':')[0]),
                    **args
                ))

        return result


@dataclass(frozen=True)
class ReceiverHandler(UnboundHandler):
    handler: Callable
    signal: Signal
    timeout: Union[int, float]
    target_class = Receiver
    _register = {}  # type: Dict[Tuple[str, str, str], ReceiverHandler]

    def __post_init__(self):
        key = self.handler.__module__, *self.handler.__qualname__.split('.')
        self._register[(*key[:-1], f'{key[-1]}:{self.signal.name}')] = self


@dataclass(frozen=True)
class ConsumerHandler(UnboundHandler):
    handler: Callable
    queue: Queue
    timeout: Union[int, float]
    options: dict
    target_class = Consumer
    _register = {}  # type: Dict[Tuple[str, str, str], ConsumerHandler]


@dataclass(frozen=True)
class PeriodicHandler(UnboundHandler):
    handler: Callable
    period: Union[int, float]
    timeout: Union[int, float]
    start_after: Union[int, float]
    target_class = PeriodicTask
    _register = {}  # type: Dict[Tuple[str, str, str], PeriodicHandler]


@dataclass(frozen=True)
class CRONHandler(UnboundHandler):
    handler: Callable
    cron: CRON
    timeout: Union[int, float]
    target_class = CRONTask
    _register = {}  # type: Dict[Tuple[str, str, str], CRONHandler]


@dataclass(frozen=True)
class HookHandler(UnboundHandler):
    handler: Callable
    label: str
    target_class = Hook
    _register = {}  # type: Dict[Tuple[str, str, str], HookHandler]
