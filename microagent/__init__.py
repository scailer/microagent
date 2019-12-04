'''
Docs
'''
__version__ = '0.9'

from collections import namedtuple
from typing import Union
import ujson

from .signal import Signal
from .queue import Queue
from .agent import MicroAgent
from .hooks import on
from .periodic_task import periodic, cron

__all__ = ['Signal', 'Queue', 'MicroAgent', 'receiver', 'consumer', 'periodic',
           'cron', 'on', 'load_stuff', 'load_signals', 'load_queues']


def load_stuff(source: str):
    '''
        Init signals from json-file loaded from disk or http request
    '''

    if source.startswith('file://'):
        with open(source.replace('file://', ''), 'r') as f:
            data = ujson.loads(f.read().replace('\n', ''))
    else:
        import requests
        data = requests.get(source).json()

    for _data in data.get('signals', []):
        Signal(name=_data['name'], providing_args=_data['providing_args'])

    for _data in data.get('queues', []):
        Queue(name=_data['name'])

    return (
        namedtuple('signals', Signal.get_all().keys())(*Signal.get_all().values()),
        namedtuple('queues', Queue.get_all().keys())(*Queue.get_all().values())
    )


def load_signals(source: str):
    return load_stuff(source)[0]


def load_queues(source: str):
    return load_stuff(source)[1]


def receiver(*signals: Union[Signal, str], timeout: int = 60):
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

    def _decorator(func):
        func.timeout = timeout
        func.__receiver__ = True

        for _signal in signals:
            if isinstance(_signal, str):
                _signal = Signal.get(_signal)
            _signal.connect(func)

        return func

    return _decorator


def consumer(queue: Queue, timeout: int = 60, **options):
    '''
        Decorator binding handler to consume messages from queue
    '''

    def _decorator(func):
        func.timeout = timeout
        func.options = options
        func.queue = queue
        func.__consumer__ = True
        return func

    return _decorator
