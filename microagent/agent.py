'''

.. code-block:: python

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
'''
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Iterable, Callable, Union, Dict, Tuple
from inspect import getmembers, ismethod
from datetime import datetime, timedelta

from .signal import Signal, Receiver
from .queue import Queue, Consumer
from .bus import AbstractSignalBus
from .broker import AbstractQueueBroker
from .hooks import Hooks
from .periodic_task import PeriodicTask, CRONTask


@dataclass(frozen=True)
class Server:
    agent: 'MicroAgent'
    handler: Callable
    endpoint: str
    options: dict


HandlerTypes = Union[Receiver, Consumer, PeriodicTask, CRONTask, Server]


class MicroAgent:
    '''
        MicroAgent

        - reactive
        - rpc
        - periodic
        - queue
        - server

        :param bus: signal bus object of subclass :class:`AbstractSignalBus`,
            required if `receiver` used
        :param broker:  queue broker object of subclass :class:`AbstractQueueBroker`,
            required if `consumer` used
        :param logger: prepared :class:`logging.Logger`,
            setup default logger if not provided
        :param settings: dict of user settings storing in object

        .. attribute:: log

            Prepared python logger::

                self.log.info('Hellow world')

        .. attribute:: bus

            Signal bus, provided on initializing::

                await self.bus.send_mail.send('agent', user_id=1)

        .. attribute:: broker

            Queue broker, provided on initializing::

                await self.broker.mailer.send({'text': 'Hellow world'})

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
    servers: Iterable[Server]
    hook: Hooks

    def __new__(cls, bus, broker, **kwargs) -> 'MicroAgent':
        agent = super(MicroAgent, cls).__new__(cls)

        agent.periodic_tasks = PeriodicHandler.bound(agent)
        agent.cron_tasks = CRONHandler.bound(agent)
        agent.receivers = ReceiverHandler.bound(agent)
        agent.consumers = ConsumerHandler.bound(agent)
        agent.servers = ServerHandler.bound(agent)
        agent.log = logging.getLogger('microagent')
        agent.hook = Hooks(agent)
        agent.settings = {}

        if agent.receivers:
            assert bus, 'Bus required'
            assert isinstance(bus, AbstractSignalBus), \
                f'Bus must be AbstractSignalBus instance or None instead {bus}'

        if agent.consumers:
            assert broker, 'Broker required'
            assert isinstance(broker, AbstractQueueBroker), \
                f'Broker must be AbstractQueueBroker instance or None instead {broker}'

        return agent

    def __init__(
            self,
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

    async def start(
            self,
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
        '''

        await self.hook.on_pre_start()

        if enable_receiving_signals:
            await self.bind_receivers(self.receivers)

        if enable_consuming_messages:
            await self.bind_consumers(self.consumers)

        if enable_periodic_tasks:
            self.run_periodic_tasks(self.periodic_tasks, self.cron_tasks)

        await self.hook.on_post_start()

        if enable_servers_running:
            await self.run_servers(self.servers)

    async def stop(self) -> None:
        await self.hook.on_pre_stop()

    async def run_servers(self, servers: Iterable[Server]) -> None:
        _servers = []

        for server in servers:
            _servers.append(server.handler(server.endpoint, **server.options))
            self.log.info('Starting server %s', server.endpoint)

        await asyncio.gather(*_servers)

    def run_periodic_tasks(self, periodic_tasks: Iterable[PeriodicTask],
            cron_tasks: Iterable[CRONTask]) -> None:

        for task in [*periodic_tasks, *cron_tasks]:
            start_after = getattr(self, 'start_after', None) or 0  # type: Union[int, float]

            if start_after > 100:
                start_at = datetime.now() + timedelta(seconds=start_after)  # type: datetime
                self.log.debug('Set %s at %s', self, f'{start_at:%H:%M:%S}')
            else:
                self.log.debug('Set %s after %d sec', self, start_after)

            task.start()

    async def bind_receivers(self, receivers: Iterable[Receiver]) -> None:
        ''' Bind signal receivers to bus subscribers '''
        if self.bus:
            for receiver in receivers:
                await self.bus.bind_receiver(receiver)

    async def bind_consumers(self, consumers: Iterable[Consumer]) -> None:
        ''' Bind message consumers to queues '''
        if self.broker:
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
                    'cron': str(task.croniter),
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
                classes.append((_cls.__module__, _cls.__name__))

        for lookup_key, unbound in cls._register.items():
            args = unbound.__dict__
            args.pop('handler', None)

            if tuple(lookup_key[:2]) in classes:
                result.append(cls.target_class(
                    agent=agent,
                    handler=getattr(agent, lookup_key[-1]),
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
    spec: str
    timeout: Union[int, float]
    target_class = CRONTask
    _register = {}  # type: Dict[Tuple[str, str, str], CRONHandler]


@dataclass(frozen=True)
class ServerHandler(UnboundHandler):
    handler: Callable
    endpoint: str
    options: dict
    target_class = Server
    _register = {}  # type: Dict[Tuple[str, str, str], ServerHandler]
