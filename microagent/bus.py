import abc
import uuid
import logging
import asyncio
import inspect
import ujson

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Union, Callable, Dict, Any, Tuple, ClassVar, Type
from datetime import datetime

from .signal import Signal, Receiver

response_signal = Signal(name='response', providing_args=[])


class ResponseContext:
    _responses: Dict[str, 'ResponseContext'] = {}
    signal_id: str
    await_from: List[str]
    timeout: int
    response: Dict[str, Any]
    fut: asyncio.Future

    def __init__(self, await_from: List[str] = None, timeout: int = 60) -> None:
        self.signal_id = uuid.uuid4().hex
        self.await_from = await_from or []
        self.timeout = timeout
        self.response = {}

    async def __aenter__(self) -> Tuple[str, asyncio.Future]:
        _loop = asyncio.get_running_loop()

        self.fut = _loop.create_future()
        self._responses[self.signal_id] = self
        _loop.call_later(self.timeout, self.close)

        return self.signal_id, self.fut

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def close(self) -> None:
        if not self.fut.done():
            self.fut.cancel()
        self._responses.pop(self.signal_id, None)

    @classmethod
    def get(cls, signal_id: str) -> Optional['ResponseContext']:
        return cls._responses.get(signal_id)

    @classmethod
    def finish(cls, signal_id: str, message: Dict[str, Union[str, int, None]]) -> None:
        resp = cls.get(signal_id)  # type: Optional[ResponseContext]

        if not resp:
            return

        if not resp.await_from:
            return resp.fut.set_result(message)

        resp.response.update(message)
        if set(resp.await_from) - set(x.split('.')[0] for x in resp.response.keys()):
            resp.fut.set_result(resp.response)


def response_context_factory() -> Type[ResponseContext]:
    '''
        Make isolated response context
    '''

    class BoundResponseContext(ResponseContext):
        _responses: dict = {}

    return BoundResponseContext


class AbstractSignalBus(abc.ABC):
    '''
        Signal bus
    '''

    RESPONSE_TIMEOUT: int = 60  # sec

    dsn: str
    prefix: str
    log: logging.Logger

    uid: str
    receivers: Dict[str, List[Receiver]]
    response_context: Type[ResponseContext]

    def __new__(cls, dsn, **kwargs) -> 'AbstractSignalBus':
        bus = super(AbstractSignalBus, cls).__new__(cls)

        bus.uid = uuid.uuid4().hex
        bus.log = logging.getLogger('microagent.bus')
        bus.receivers = defaultdict(list)

        # isolate responses context for preventing rase of handlers
        bus.response_context = response_context_factory()

        return bus

    def __init__(self, dsn: str, prefix: str = 'PUBSUB', logger: logging.Logger = None) -> None:

        self.dsn = dsn
        self.prefix = prefix

        if logger:
            self.log = logger

        asyncio.create_task(self.bind(response_signal.make_channel_name(self.prefix)))

        self.log.debug('%s initialized', self)

    def __repr__(self) -> str:
        return f'<Bus {self.__class__.__name__} {self.dsn} {self.prefix}#{self.uid}>'

    def __getattr__(self, name: str) -> 'BoundSignal':
        signal = Signal.get(name)
        return BoundSignal(self, signal)

    @abc.abstractmethod
    def send(self, channel: str, message: str):
        return NotImplemented  # pragma: no cover

    @abc.abstractmethod
    def bind(self, signal: str):
        return NotImplemented  # pragma: no cover

    async def bind_receiver(self, receiver: Receiver) -> None:
        self.receivers[receiver.signal.name].append(receiver)
        self.log.info('Bind %s to %s: %s', receiver.signal, self, receiver)
        if receiver.signal.name not in self.receivers:
            await self.bind(receiver.signal.make_channel_name(self.prefix))

    @abc.abstractmethod
    def receiver(self, *args, **kwargs):
        return NotImplemented  # pragma: no cover

    async def call(self, channel: str, message: str, await_from: List[str] = None) -> Any:

        async with self.response_context(await_from, self.RESPONSE_TIMEOUT) as (signal_id, future):
            await self.send(f'{channel}#{signal_id}', message)
            return await future

    def _receiver(self, channel: str, message: str) -> None:
        signal_id = None  # type: Optional[str]

        if '#' in channel:
            channel, signal_id = channel.split('#')

        pref, name, sender = channel.split(':')
        signal = Signal.get(name)  # type: Signal

        try:
            data = signal.deserialize(message)  # type: dict
        except ValueError:
            self.log.error('Invalid pubsub message: %s', data)
            return

        if not isinstance(data, dict):
            self.log.error('Invalid pubsub message: not dict')
            return

        if name == 'response' and signal_id:
            return self.handle_response(signal_id, data)

        diff_args = set(signal.providing_args) ^ set(data.keys())

        if diff_args:
            self.log.warning('Pubsub mismatch arguments %s %s', channel, diff_args)

        asyncio.create_task(self.handle_signal(signal, sender, signal_id, data))

    def handle_response(self, signal_id: str, message: Dict[str, Union[int, str, None]]) -> None:
        try:
            self.response_context.finish(signal_id, message)
        except asyncio.InvalidStateError as exc:
            self.log.error('Response handle failed: %s', exc, exc_info=True)

    async def handle_signal(self, signal: Signal, sender: str,
            signal_id: Optional[str], message: dict) -> None:

        receivers = self.receivers.get(signal.name, [])  # type: List[Receiver]

        responses = await asyncio.gather(*[
            self.broadcast(receiver, signal, sender, message)
            for receiver in receivers
        ])  # type: List[Union[int, str, None]]

        if signal_id:
            await self.send(
                f'{self.prefix}:response:{self.uid}#{signal_id}',
                ujson.dumps({
                    rec.handler: res for rec, res in zip(receivers, responses)
                })
            )

    async def broadcast(self, receiver: Receiver, signal: Signal,
            sender: str, message: dict) -> Union[int, str, None]:

        self.log.debug('Calling %s by %s:%s with %s', receiver.handler,
            signal.name, sender, str(message).encode('utf-8'))

        try:
            response = receiver.handler(signal=signal, sender=sender, **message)
        except TypeError:
            self.log.error('Call %s failed', signal.name, exc_info=True)
            return None

        if inspect.isawaitable(response):
            timer = datetime.now().timestamp()  # type: float

            try:
                response = await asyncio.wait_for(response, receiver.timeout)
                if isinstance(response, (int, str)):
                    return response

            except asyncio.TimeoutError:
                self.log.error(
                    'TimeoutError: %s %.2f', receiver.handler,
                    datetime.now().timestamp() - timer)

        return None


class BoundSignal:
    __slots__ = ('bus', 'signal')

    def __init__(self, bus: AbstractSignalBus, signal: Signal):
        self.bus = bus
        self.signal = signal

    async def send(self, sender: str, **kwargs: Any) -> None:
        await self.bus.send(
            self.signal.make_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs))

    async def call(self, sender: str, **kwargs: Any) -> Dict[str, Union[str, int, None]]:
        return await self.bus.call(
            self.signal.make_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs))
