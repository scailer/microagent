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
import inspect
from functools import partial, wraps
from collections import defaultdict
from typing import Optional, Iterable
from inspect import getmembers, ismethod
from datetime import datetime, timedelta

from copy import copy

from .signal import Signal
from .bus import AbstractSignalBus
from .broker import AbstractQueueBroker


def on(*hooks: str):
    '''
        Bind to internal event
    '''

    def _decorator(func):
        func.__hook__ = [f'on_{hook}' for hook in hooks]
        return func

    return _decorator


def hook_decorator(hook):
    def decorator(method):
        if not any([
            f'on_pre_{method.__name__}' in hook.bindings,
            f'on_error_{method.__name__}' in hook.bindings,
            f'on_post_{method.__name__}' in hook.bindings
        ]):
            return method

        @wraps(method)
        async def wrapper(*args, **kwargs):
            context = {'args': args, 'kwargs': kwargs}

            if f'on_pre_{method.__name__}' in hook.bindings:
                await hook.call_hook(f'on_pre_{method.__name__}', context=context)

            try:
                response = method(*args, **kwargs)

                if inspect.isawaitable(response):
                    response = await response

            except Exception as exc:
                if f'on_error_{method.__name__}' in hook.bindings:
                    await hook.call_hook(f'on_error_{method.__name__}', exc, context=context)
                raise

            if f'on_post_{method.__name__}' in hook.bindings:
                await hook.call_hook(f'on_post_{method.__name__}', response, context=context)

            return response
        return wrapper
    return decorator


class Hooks:
    '''
        pre|error|post:method
    '''
    def __init__(self, agent):
        binded_methods = defaultdict(list)

        for _, method in getmembers(agent, ismethod):
            if hasattr(method, '__hook__'):
                for hook_name in method.__hook__:
                    binded_methods[hook_name].append(method)

        self._bindings = dict(binded_methods)

    @property
    def bindings(self):
        return self._bindings

    @property
    def decorate(self):
        return hook_decorator(self)

    async def call_hook(self, name, *args, **kwargs):
        methods = self._bindings.get(name)
        if methods:
            for method in methods:
                response = method(*args, **kwargs)
                if inspect.isawaitable(response):
                    await response

    def __getattr__(self, name: str):
        if name.startswith('on_'):
            return partial(self.call_hook, name)

        return getattr(super(), name)


class MicroAgent:
    '''
        MicroAgent

        - reactive
        - rpc
        - periodic
        - queue

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

        self.received_signals = self._get_received_signals()
        if self.received_signals:
            assert self.bus, 'Bus required'
            assert isinstance(self.bus, AbstractSignalBus), \
                f'Bus must be AbstractSignalBus instance or None instead {bus}'

        self.queue_consumers = self._get_queue_consumers()
        if self.queue_consumers:
            assert self.broker, 'Broker required'
            assert isinstance(self.broker, AbstractQueueBroker), \
                f'Broker must be AbstractQueueBroker instance or None instead {broker}'

        self.log.debug('%s initialized', self)

    async def start(
            self,
            enable_periodic_tasks: Optional[bool] = True,
            enable_receiving_signals: Optional[bool] = True,
            enable_consuming_messages: Optional[bool] = True):
        '''
            Starting MicroAgent to receive signals, consume messages
            and initiate periodic running.

            :param enable_periodic_tasks: default enabled
            :param enable_consuming_messages: default enabled
            :param enable_receiving_signals: default enabled
        '''

        await self.hook.on_pre_start()

        if enable_receiving_signals:
            await self.bind_receivers(self.received_signals.values())

        if enable_consuming_messages:
            await self.bind_consumers(self.queue_consumers)

        if enable_periodic_tasks:
            self.run_periodic_tasks(self._periodic_tasks)

        await self.hook.on_post_start()

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

    def _get_received_signals(self):
        signals = {}
        mod, name = self.__module__, self.__class__.__name__

        for signal in Signal.get_all().values():
            receivers = []

            for lookup_key, _ in signal.receivers:
                if lookup_key.mod == mod and lookup_key.name.startswith(name):
                    funcname = lookup_key.name.replace(name, '')[1:]
                    func = getattr(self, funcname, None)
                    if func:
                        receivers.append((lookup_key, hook_decorator(self.hook)(func)))

            if receivers:
                signal = copy(signal)
                signal.receivers = receivers
                signals[signal.name] = signal

        return signals

    async def bind_receivers(self, signals: Iterable[Signal]):
        ''' Bind signal receivers to bus subscribers '''
        for signal in signals:
            await self.bus.bind_signal(signal)

    async def bind_consumers(self, consumers: Iterable):
        ''' Bind message consumers to queues '''
        for consumer in consumers:
            await self.broker.bind_consumer(consumer)
