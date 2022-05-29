__version__ = '1.6.1'

import json
import importlib
import urllib.request
from collections import namedtuple
from typing import Union, Tuple, Callable, Dict, Iterable, Any, Optional, List

from .signal import Signal
from .queue import Queue
from .launcher import ServerInterrupt
from .periodic_task import cron_parser
from .agent import (MicroAgent, ReceiverHandler, ConsumerHandler,
    PeriodicHandler, CRONHandler, HookHandler)

__all__ = ['Signal', 'Queue', 'MicroAgent', 'ServerInterrupt', 'receiver', 'consumer',
           'periodic', 'cron', 'on', 'load_stuff', 'load_signals', 'load_queues']

JSON_TYPES = {
    'string': str,
    'number': int,
    'object': dict,
    'array': list,
    'boolean': bool,
    'null': type(None)
}


def get_types(value: Union[str, List[str]]) -> Tuple[type, ...]:
    if isinstance(value, str):
        value = [value]
    return tuple(JSON_TYPES[x] for x in value)


def load_stuff(source: str) -> Tuple[Any, Any]:
    '''
        Init signals from json-file loaded from disk or http request
    '''

    data: Dict[str, Iterable[Dict[str, Any]]] = {}

    if source.startswith('file://'):
        with open(source.replace('file://', ''), 'r') as f:
            data.update(json.loads(f.read().replace('\n', '')))
    else:
        with urllib.request.urlopen(source) as response:
            data.update(json.loads(response.read()))

    for _data in data.get('signals', []):
        providing_args, type_map = _data['providing_args'], None

        if isinstance(providing_args, dict):
            type_map = {name: get_types(_type) for name, _type in providing_args.items()}
            providing_args = list(providing_args)

        Signal(name=_data['name'], providing_args=providing_args, type_map=type_map)

    for _data in data.get('queues', []):
        Queue(name=_data['name'])

    if data.get('jsonlib'):
        jsonlib = importlib.import_module(data['jsonlib'])  # type: ignore
        Signal.set_jsonlib(jsonlib)
        Queue.set_jsonlib(jsonlib)

    return (
        namedtuple('signals', Signal.get_all().keys())(*Signal.get_all().values()),  # type: ignore
        namedtuple('queues', Queue.get_all().keys())(*Queue.get_all().values())  # type: ignore
    )


def load_signals(source: str) -> Any:
    '''
        Load Signal-entities from file or by web.

        .. code-block:: python

            from microagent import load_signals

            signals_from_file = load_signals('file://signals.json')
            signals_from_web = load_signals('http://example.com/signals.json')


        Signals declarations (signals.json).

        .. code-block:: json

            {
                "signals": [
                    {"name": "started", "providing_args": []},
                    {"name": "user_created", "providing_args": ["user_id"]},
                    {"name": "typed_signal", "providing_args": {
                        "uuid": "string",
                        "code": ["number", "null"],
                        "flag": "boolean",
                        "ids": "array"
                    }}
                ]
            }
    '''
    return load_stuff(source)[0]


def load_queues(source: str) -> Any:
    '''
        Load Queue-entities from file or by web.

        .. code-block:: python

            from microagent import load_queues

            signals_from_file = load_signals('file://queues.json')
            signals_from_web = load_signals('http://example.com/queues.json')


        Queues declarations (queues.json).

        .. code-block:: json

            {
                "queues": [
                    {"name": "mailer"},
                    {"name": "pusher"},
                ]
            }
    '''
    return load_stuff(source)[1]


def periodic(period: Union[int, float], timeout: Union[int, float] = 1,
        start_after: Union[int, float] = 0) -> Callable:
    '''
        Run decorated handler periodically.

        :param period: Period of running functions in seconds
        :param timeout: Function timeout in seconds
        :param start_after: Delay for running loop in seconds

        .. code-block:: python

            @periodic(period=5)
            async def handler_1(self):
                log.info('Called handler 1')

            @periodic(5, timeout=4)
            async def handler_2(self):
                log.info('Called handler 2')

            @periodic(period=5, start_after=10)
            async def handler_3(self):
                log.info('Called handler 3')
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
        Run decorated function by schedule (cron)

        :param spec: Specified running scheduling in cron format
        :param timeout: Function timeout in seconds

        .. code-block:: python

            @periodic('0 */4 * * *')
            async def handler_1(self):
                log.info('Called handler 1')

            @periodic('*/15 * * * *', timeout=10)
            async def handler_2(self):
                log.info('Called handler 2')
    '''

    assert timeout > 0.001, 'timeout must be more than 0.001 s'

    def _decorator(func):
        func._cron = CRONHandler(
            handler=func,
            cron=cron_parser(spec),
            timeout=float(timeout)
        )
        return func

    return _decorator


def receiver(*signals: Signal, timeout: int = 60) -> Callable:
    '''
        Binding for signals receiving.

        Handler can receive **many** signals, and **many** handlers can receiver same signal.

        :param signals: List of receiving signals
        :param timeout: Calling timeout in seconds

        .. code-block:: python

            @receiver(signal_1, signal_2)
            async def handler_1(self, **kwargs):
                log.info('Called handler 1 %s', kwargs)

            @receiver(signal_1)
            async def handle_2(self, **kwargs):
                log.info('Called handler 2 %s', kwargs)

            @receiver(signal_2, timeout=30)
            async def handle_3(self, **kwargs):
                log.info('Called handler 3 %s', kwargs)
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


def consumer(queue: Queue, timeout: int = 60, dto_class: Optional[type] = None,
        dto_name: Optional[str] = None, **options) -> Callable:
    '''
        Binding for consuming messages from queue.

        Only **one** handler can be bound to **one** queue.

        :param queue: Queue - source of data
        :param timeout: Calling timeout in seconds
        :param dto_class: DTO-class, wrapper for consuming data
        :param dto_name: DTO name in consumer method kwargs

        .. code-block:: python

            @consumer(queue_1)
            async def handler_1(self, **kwargs):
                log.info('Called handler 1 %s', kwargs)

            @consumer(queue_2, timeout=30)
            async def handle_2(self, **kwargs):
                log.info('Called handler 2 %s', kwargs)

            @consumer(queue_3, dto_class=MyDTO)
            async def handle_3(self, dto: MyDTO, **kwargs):
                log.info('Called handler 3 %s', dto)  # dto = MyDTO(**kwargs)

            @consumer(queue_4, timeout=30, dto_class=MyDTO, dto_name='obj')
            async def handle_4(self, obj: MyDTO, **kwargs):
                log.info('Called handler 4 %s', obj)  # obj = MyDTO(**kwargs)
    '''

    def _decorator(func):
        func._consumer = ConsumerHandler(
            handler=func,
            queue=queue,
            timeout=timeout,
            dto_class=dto_class,
            dto_name=dto_name,
            options=options
        )
        return func

    return _decorator


def on(label: str) -> Callable:
    '''
        Hooks for internal events *(pre_start, post_start, pre_stop)*
        or running forever servers *(server)*.

        Server-function will be call as run-forever asyncio task.

        :param label: Hook type label string *(pre_start, post_start, pre_stop, server)*

        .. code-block:: python

            @on('pre_start')
            async def handler_1(self):
                log.info('Called handler 1')

            @on('post_start')
            async def handler_2(self):
                log.info('Called handler 2')

            @on('pre_stop')
            async def handler_3(self):
                log.info('Called handler 3')

            @on('server')
            async def run_server(self):
                await Server().start()  # run forever
                raise ServerInterrupt('Exit')  # graceful exit
    '''
    assert label in ('pre_start', 'post_start', 'pre_stop', 'server'), 'Bad label'

    def _decorator(func):
        func._hook = HookHandler(
            handler=func,
            label=label
        )
        return func

    return _decorator
