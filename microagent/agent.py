import asyncio
import logging
from typing import Optional, List, Union

from copy import copy

from .signal import Signal
from .bus import AbstractSignalBus


class MicroAgent:
    '''
        MicroAgent

        - reactive
        - rpc
        - periodic
    '''

    def __init__(self, bus: AbstractSignalBus, logger: Optional[logging.Logger] = None,
            settings: Optional[dict] = None):

        self._loop = asyncio.get_event_loop()
        self._periodic_tasks = self._get_periodic_tasks()
        self.log = logger or logging.getLogger('microagent')
        self.settings = settings or {}
        self.bus = bus

        self.received_signals = self._get_received_signals()
        if self.received_signals:
            asyncio.ensure_future(self.bind_receivers(self.received_signals.values()))

        self.setup()

        for method in self._periodic_tasks:
            self._loop.call_later(getattr(method, '_start_after'), method)

    def setup(self):
        pass

    def info(self):
        return {
            'name': self.__class__.__name__,
            'bus': str(self.bus),
            'periodic': [
                {'name': x.origin.__name__, 'period': x.origin._period,
                 'timeout': x.origin._timeout, 'start_after': x._start_after}
                 for x in self._periodic_tasks
            ],
            'receivers': [
                {'signal': k, 'receivers': [
                    {'name': x[1].__name__, 'key': x[0]} for x in v.receivers
                ]} for k, v in self.received_signals.items()
            ]
        }

    def _get_periodic_tasks(self):
        _periodic_tasks = []

        for method_name in dir(self):
            if method_name.startswith('_'):
                continue

            method = getattr(self, method_name)
            if isinstance(getattr(method, '_start_after', None), int):
                _periodic_tasks.append(method)

        return _periodic_tasks

    def _get_received_signals(self):
        signals, mod, name = {}, self.__module__, self.__class__.__name__

        for signal in Signal._signals.values():
            receivers = []

            for key, _ in signal.receivers:
                if key[0] == mod and key[1].startswith(name):
                    funcname = key[1].replace(name, '')[1:]
                    func = getattr(self, funcname, None)
                    if func:
                        receivers.append((key, func))

            if receivers:
                signal = copy(signal)
                signal.receivers = receivers
                signals[signal.name] = signal

        return signals

    async def bind_receivers(self, signals: Union[Signal, List[Signal]]):
        ''' Bind signal receivers to bus subscribers '''
        for signal in signals:
            await self.bus.bind(signal)
