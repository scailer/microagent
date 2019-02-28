import asyncio
import logging
import inspect
from functools import partial
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


class Hooks:
    def __init__(self, agent):
        binded_methods = defaultdict(list)

        for _, method in getmembers(agent, ismethod):
            if hasattr(method, '__hook__'):
                for hook_name in method.__hook__:
                    binded_methods[hook_name].append(method)

        self._bindings = dict(binded_methods)

    async def _call_hook(self, name, *args, **kwargs):
        methods = self._bindings.get(name)
        if methods:
            for method in methods:
                response = method(*args, **kwargs)
                if inspect.isawaitable(response):
                    await response

    def __getattr__(self, name: str):
        if name.startswith('on_'):
            return partial(self._call_hook, name)

        return getattr(super(), name)


class MicroAgent:
    '''
        MicroAgent

        - reactive
        - rpc
        - periodic
        - queue
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
            method
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
                        receivers.append((lookup_key, func))

            if receivers:
                signal = copy(signal)
                signal.receivers = receivers
                signals[signal.name] = signal

        return signals

    async def bind_receivers(self, signals: Iterable[Signal]):
        ''' Bind signal receivers to bus subscribers '''
        for signal in signals:
            print('SIGNAL', signal, self.bus.bind_signal)
            await self.bus.bind_signal(signal)

    async def bind_consumers(self, consumers: Iterable):
        for consumer in consumers:
            await self.broker.bind_consumer(consumer)
