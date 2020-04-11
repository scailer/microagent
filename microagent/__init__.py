'''
Docs
'''
__version__ = '0.10'

from collections import namedtuple
from typing import Union, Tuple, Callable, Dict, Iterable, List, Any
import importlib
import time

from croniter import croniter
import ujson

from .signal import Signal
from .queue import Queue
from .agent import MicroAgent, ReceiverHandler, ConsumerHandler, PeriodicHandler, CRONHandler
from .hooks import on

__all__ = ['Signal', 'Queue', 'MicroAgent', 'receiver', 'consumer', 'periodic',
           'endpoint', 'cron', 'on', 'load_stuff', 'load_signals', 'load_queues']


def load_stuff(source: str) -> Tuple[object, object]:
    '''
        Init signals from json-file loaded from disk or http request
    '''

    data: Dict[str, Iterable[Dict[str, Any]]] = {}

    if source.startswith('file://'):
        with open(source.replace('file://', ''), 'r') as f:
            data.update(ujson.loads(f.read().replace('\n', '')))
    else:
        import requests
        data.update(requests.get(source).json())

    for _data in data.get('signals', []):
        Signal(name=_data['name'], providing_args=_data['providing_args'])

    for _data in data.get('queues', []):
        Queue(name=_data['name'])

    return (
        namedtuple('signals', Signal.get_all().keys())(*Signal.get_all().values()),  # type: ignore
        namedtuple('queues', Queue.get_all().keys())(*Queue.get_all().values())  # type: ignore
    )


def load_signals(source: str) -> object:
    return load_stuff(source)[0]


def load_queues(source: str) -> object:
    return load_stuff(source)[1]


def periodic(period: Union[int, float], timeout: Union[int, float] = 1,
        start_after: Union[int, float] = 0) -> Callable:
    '''
        Decorator witch mark :class:`MicroAgent` method as periodic function

        :param period: Period of running functions in seconds
        :param timeout: Function timeout in seconds
        :param start_after: Delay for running loop in seconds
    '''

    assert period > 0.001, 'period must be more than 0.001 s'
    assert timeout > 0.001, 'timeout must be more than 0.001 s'
    assert start_after >= 0, 'start_after must be a positive'

    def _decorator(func):
        func._periodic = PeriodicHandler(
            handler=func,
            period=float(period),
            timeout=float(timeout),
            start_after=float(start_after)
        )
        return func

    return _decorator


def cron(spec: str, timeout: Union[int, float] = 1) -> Callable:
    '''
        Decorator witch mark :class:`MicroAgent` method as shceduling (cron) function

        :param spec: Specified running shceduling in cron format
        :param timeout: Function timeout in seconds
    '''

    assert timeout > 0.001, 'timeout must be more than 0.001 s'

    def _decorator(func):
        func._cron = CRONHandler(
            handler=func,
            croniter=croniter(spec, time.time()),
            timeout=float(timeout)
        )
        return func

    return _decorator


def receiver(*signals: Union[Signal, str], timeout: int = 60) -> Callable:
    '''
        Decorator binding handler to receiving signals

        @receiver(signal_1, signal_2)
        async def handler_1(self, **kwargs):
            log.info('Called handler 1 %s', kwargs)

        @receiver(signal_1)
        async def handle_2(self, **kwargs):
            log.info('Called handler 2 %s', kwargs)
    '''

    def _decorator(func):
        for _signal in signals:
            func._receiver = ReceiverHandler(
                handler=func,
                signal=_signal,
                timeout=timeout
            )

        return func

    return _decorator


def consumer(queue: Queue, timeout: int = 60, **options) -> Callable:
    '''
        Decorator binding handler to consume messages from queue
    '''

    def _decorator(func):
        func._consumer = ConsumerHandler(
            handler=func,
            queue=queue,
            timeout=timeout,
            options=options
        )
        return func

    return _decorator


def endpoint(url, **options) -> Callable:
    '''
        Decorator marking handler as subserver runner.
        Will run in start() method, and make it runnig forever for transperent
        error passing.
    '''

    def _decorator(func):
        func.options = options
        func.endpoint = url
        func.__server__ = True
        return func

    return _decorator
