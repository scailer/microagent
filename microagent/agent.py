import asyncio
import logging
from typing import Optional, List
from inspect import getmembers, ismethod

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
    log = logging.getLogger('microagent')

    def __init__(
            self,
            bus: AbstractSignalBus,
            logger: logging.Logger = None,
            settings: dict = None,
            enable_periodic_tasks: Optional[bool] = True,
            enable_receiving_signals: Optional[bool] = True
        ):

        self._loop = asyncio.get_event_loop()
        self._periodic_tasks = self._get_periodic_tasks()
        self.settings = settings or {}
        self.bus = bus

        if logger:
            self.log = logger

        self.received_signals = self._get_received_signals()
        if enable_receiving_signals and self.received_signals:
            asyncio.ensure_future(
                self.bind_receivers(list(self.received_signals.values())))

        self.setup()

        if enable_periodic_tasks:
            for method in self._periodic_tasks:
                self._loop.call_later(getattr(method, '_start_after'), method)

    def setup(self):
        pass

    def info(self):
        return {
            'name': self.__class__.__name__,
            'bus': str(self.bus),
            'periodic': [
                {'name': func.origin.__name__, 'period': func.origin._period,
                 'timeout': func.origin._timeout, 'start_after': func._start_after}
                for func in self._periodic_tasks
            ],
            'receivers': [
                {'signal': signal, 'receivers': [
                    {'name': receiver[1].__name__, 'key': receiver[0]}
                    for receiver in val.receivers
                ]} for signal, val in self.received_signals.items()
            ]
        }

    def _get_periodic_tasks(self):
        return tuple(
            method
            for name, method in getmembers(self, ismethod)
            if hasattr(method, '__periodic__')
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

    async def bind_receivers(self, signals: List[Signal]):
        ''' Bind signal receivers to bus subscribers '''
        for signal in signals:
            await self.bus.bind_signal(signal)
