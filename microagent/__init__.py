from collections import namedtuple
from typing import List, Union
import ujson

from .signal import Signal
from .agent import MicroAgent
from .periodic_task import periodic

__all__ = ['Signal', 'MicroAgent', 'load_signals', 'receiver', 'periodic']


def load_signals(source: str):
    '''
        Init signals from json-file loaded from disk or http request
    '''

    if source.startswith('file://'):
        with open(source.replace('file://', ''), 'r') as f:
            data = ujson.loads(f.read().replace('\n', ''))
    else:
        import requests
        data = requests.get(source).json()

    for _data in data['signals']:
        Signal(name=_data['name'], providing_args=_data['providing_args'])

    return namedtuple('signals', Signal.get_all().keys())(*Signal._signals.values())


def receiver(signal: Union[Signal, List[Signal], str, List[str]], timeout: int = 60):
    '''
        Decorator binding handler to receiving signals

        @receiver([signal_1, signal_2])
        async def handler_1(self, **kwargs):
            log.info('Called handler 1 %s', kwargs)

        @receiver(signal_1)
        async def handle_2(self, **kwargs):
            log.info('Called handler 2 %s', kwargs)

        @receiver('signal_1')
        async def handle_3(self, **kwargs):
            log.info('Called handler 3 %s', kwargs)
    '''

    if not isinstance(signal, (list, tuple)):
        signal = [signal]

    def _decorator(func):
        func.timeout = timeout
        for _signal in signal:
            if isinstance(_signal, str):
                _signal = Signal.get(_signal)
            _signal.connect(func)
        return func

    return _decorator
