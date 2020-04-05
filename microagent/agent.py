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
from typing import Optional, Iterable, Callable, Union
from inspect import getmembers, ismethod
from datetime import datetime, timedelta

from copy import copy

from .signal import Signal
from .queue import Queue
from .bus import AbstractSignalBus
from .broker import AbstractQueueBroker
from .hooks import Hooks


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

        .. attribute:: _loop

            Event loop
    '''
    log = logging.getLogger('microagent')

    def __init__(
            self,
            bus: AbstractSignalBus = None,
            broker: AbstractQueueBroker = None,
            logger: logging.Logger = None,
            settings: dict = None):

        self.hook = Hooks(self)
        self._loop = asyncio.get_event_loop()
        self.settings = settings or {}
        self.broker = broker
        self.bus = bus

        if logger:
            self.log = logger

        self._periodic_tasks = self._get_periodic_tasks()

        self.receivers = ReceiverHandler.bound(self)
        if self.receivers:
            assert self.bus, 'Bus required'
            assert isinstance(self.bus, AbstractSignalBus), \
                f'Bus must be AbstractSignalBus instance or None instead {bus}'

        self.queue_consumers = self._get_queue_consumers()
        if self.queue_consumers:
            assert self.broker, 'Broker required'
            assert isinstance(self.broker, AbstractQueueBroker), \
                f'Broker must be AbstractQueueBroker instance or None instead {broker}'

        self._servers = self._get_servers()

        self.log.debug('%s initialized', self)

    async def start(
            self,
            enable_periodic_tasks: Optional[bool] = True,
            enable_receiving_signals: Optional[bool] = True,
            enable_consuming_messages: Optional[bool] = True,
            enable_servers_running: Optional[bool] = True):
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
            await self.bind_consumers(self.queue_consumers)

        if enable_periodic_tasks:
            self.run_periodic_tasks(self._periodic_tasks)

        await self.hook.on_post_start()

        if enable_servers_running:
            await self.run_servers(self._servers)

    async def stop(self):
        await self.hook.on_pre_stop()

    async def run_servers(self, servers):
        _servers = []

        for func in self._servers:
            _servers.append(func(func.endpoint, **func.options))
            self.log.info('Starting server %s', func.endpoint)

        await asyncio.gather(*_servers)

    def run_periodic_tasks(self, periodic_tasks):
        for method in periodic_tasks:
            start_after = getattr(method, '_start_after', 0) or 0

            if start_after > 100:
                start_at = datetime.now() + timedelta(seconds=start_after)
                self.log.debug('Set periodic task %s at %s', method, f'{start_at:%H:%M:%S}')
            else:
                self.log.debug('Set periodic task %s after %d sec', method, start_after)

            self._loop.call_later(start_after, method)

    def __repr__(self):
        return f'<MicroAgent {self.__class__.__name__}>'

    def info(self):
        '''
            Information about MicroAgent in json-serializable dict
        '''
        return {
            'name': self.__class__.__name__,
            'bus': str(self.bus) if self.bus else None,
            'broker': str(self.broker) if self.broker else None,
            'periodic': [
                {
                    'name': func.origin.__name__,
                    'period': getattr(func.origin, '_period', None),
                    'cron': getattr(func.origin, '_croniter', None),
                    'timeout': func.origin._timeout,
                    'start_after': func._start_after,
                }
                for func in self._periodic_tasks
            ],
            'receivers': [
                {
                    signal_name: {
                        'signal': val,
                        'receivers': [
                            {
                                'key': f'{key.mod}.{key.name}',
                                'timeout': receiver.timeout,
                            }
                            for key, receiver in val.receivers
                        ]
                    }
                }
                for signal_name, val in self.received_signals.items()
            ],
            'consumers': [
                {
                    'ddd': func,
                    'queue': func.queue,
                    'timeout': func.timeout,
                    'options': func.options,
                }
                for func in self.queue_consumers
            ],
        }

    def _get_periodic_tasks(self):
        handlers = ReceiverHandler.bound(self)
        print('HHH', handlers)
        return tuple(
            method
            for name, method in getmembers(self, ismethod)
            if hasattr(method, '__periodic__')
        )

    def _get_queue_consumers(self):
        return tuple(
            self.hook.decorate(method)
            for name, method in getmembers(self, ismethod)
            if hasattr(method, '__consumer__')
        )

    def _get_servers(self):
        return tuple(
            method
            for name, method in getmembers(self, ismethod)
            if hasattr(method, '__server__')
        )

    def _get_received_signals(self):
        signals, classes = {}, []

        # Agent class with heirs
        for _cls in self.__class__.__mro__:
            if _cls.__module__ != 'builtins' and _cls.__name__ != 'MicroAgent':
                classes.append((_cls.__module__, _cls.__name__))

        for signal in Signal.get_all().values():
            receivers = []

            for lookup_key, _ in signal.receivers:
                _module, _class, funcname = lookup_key.mod, *lookup_key.name.split('.')

                if (_module, _class) in classes:
                    func = getattr(self, funcname, None)

                    if func:
                        receivers.append((lookup_key, self.hook.decorate(func)))

            if receivers:
                signal = copy(signal)
                signal.receivers = receivers
                signals[signal.name] = signal

        return signals

    async def bind_receivers(self, receivers: Iterable[ReceiverHandler]):
        ''' Bind signal receivers to bus subscribers '''
        signals = set(receiver.handler.signal for receiver in receivers)

        for signal in signals:
            await self.bus.bind_signal(signal)

    async def bind_consumers(self, consumers: Iterable):
        ''' Bind message consumers to queues '''
        for consumer in consumers:
            await self.broker.bind_consumer(consumer)


@dataclass(frozen=True)
class UnboundHandler:
    handler: Callable
    _register = {}

    def __post_init__(self):
        key = self.handler.__module__, *self.handler.__qualname__.split('.')
        self._register[key] = self

    @classmethod
    def bound(cls, agent):
        classes, result = [], []

        for _cls in agent.__class__.__mro__:
            if _cls.__module__ != 'builtins' and _cls.__name__ != 'MicroAgent':
                classes.append((_cls.__module__, _cls.__name__))

        for lookup_key, unbound in cls._register.items():
            if tuple(lookup_key[:2]) in classes:
                result.append(BoundHandler(agent=agent, handler=unbound))

        return result


@dataclass(frozen=True)
class ReceiverHandler(UnboundHandler):
    signal: Signal
    timeout: Union[int, float]
    _register = {}


@dataclass(frozen=True)
class ConsumerHandler(UnboundHandler):
    queue: Queue
    timeout: Union[int, float]
    options: dict
    _register = {}


@dataclass(frozen=True)
class PeriodicHandler(UnboundHandler):
    period: Union[int, float]
    timeout: Union[int, float]
    start_after: Union[int, float]
    _register = {}


@dataclass(frozen=True)
class CRONHandler(UnboundHandler):
    spec: str
    timeout: Union[int, float]
    _register = {}


@dataclass(frozen=True)
class EndpointHandler(UnboundHandler):
    url: str
    options: dict
    _register = {}


@dataclass(frozen=True)
class BoundHandler:
    agent: MicroAgent
    handler: UnboundHandler
