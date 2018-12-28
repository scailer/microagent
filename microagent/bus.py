import abc
import uuid
import logging
import asyncio
import ujson

from typing import Optional, List, Union, Callable
from datetime import datetime

from .signal import Signal


class ResponseContext:
    _responses = {}

    def __init__(self, loop=None, timeout=60):
        self._loop = loop or asyncio.get_event_loop()
        self.signal_id = uuid.uuid4().hex
        self.timeout = timeout
        self.fut = None

    async def __aenter__(self):
        self.fut = self._loop.create_future()
        self._responses[self.signal_id] = self
        self._loop.call_later(self.timeout, self.close)
        return (self.signal_id, self.fut)

    async def __aexit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        if not self.fut.done():
            self.fut.cancel()
        self._responses.pop(self.signal_id, None)

    @classmethod
    def get(cls, signal_id):
        return cls._responses.get(signal_id)

    @classmethod
    def set(cls, signal_id, message):
        resp = cls.get(signal_id)
        if resp:
            resp.fut.set_result(message)


class AbstractSignalBus(abc.ABC):
    '''
        Signal bus
    '''

    RESPONSE_TIMEOUT = 60  # sec
    HANDLING_TIMEOUT = 60  # sec

    def __init__(self, dsn: str, prefix: Optional[str] = 'PUBSUB',
            logger: Optional[logging.Logger] = None):

        self.received_signals = {}
        self.dsn = dsn
        self.prefix = prefix
        self.log = logger or logging.getLogger('microagent.bus')
        self._loop = asyncio.get_event_loop()
        self._responses = {}

        asyncio.ensure_future(self.bind(Signal(name='response', providing_args=[])))

    def __repr__(self):
        return '<Bus {} {}>'.format(self.__class__.__name__, self.prefix)

    def __getattr__(self, name):
        signal = Signal.get(name)
        return BoundSignal(self, signal)

    @abc.abstractmethod
    def send(self, channel: str, message: str):
        return NotImplemented

    @abc.abstractmethod
    def bind(self, signal: Signal):
        return NotImplemented

    @abc.abstractmethod
    def receiver(self, *args, **kwargs):
        return NotImplemented

    async def call(self, channel: str, message: str):
        async with ResponseContext(self._loop, self.RESPONSE_TIMEOUT) as (signal_id, future):
            await self.send(f'{channel}#{signal_id}', message)
            return await future

    def _receiver(self, channel, message):
        if '#' in channel:
            channel, signal_id = channel.split('#')
        else:
            signal_id = None

        pref, name, sender = channel.split(':')
        signal = self.received_signals.get(name)

        try:
            message = signal.deserialize(message)
        except ValueError:
            self.log.error('Invalid pubsub message: %s', message)
            return

        if not isinstance(message, dict):
            self.log.error('Invalid pubsub message: not dict')
            return

        if name == 'response' and signal_id:
            return self.handle_response(signal_id, message)

        diff_args = set(signal.providing_args) ^ set(message.keys())

        if diff_args:
            self.log.warn('Pubsub mismatch arguments %s %s', channel, diff_args)

        asyncio.ensure_future(
            self.handle_signal(signal, sender, signal_id, message),
            loop=self._loop)

    def handle_response(self, signal_id: str, message: str):
        ResponseContext.set(signal_id, message)

    async def handle_signal(self, signal: Signal, sender: str,
            signal_id: str, message: dict):

        receivers = [x[1] for x in signal.receivers]
        responses = await asyncio.gather(
            *[self.broadcast(receiver, signal, sender, message)
              for receiver in receivers], loop=self._loop)

        if signal_id:
            responses = {rec.__qualname__: res for rec, res in zip(receivers, responses)}
            channel = '{}:response:ma#{}'.format(self.prefix, signal_id)
            await self.send(channel, ujson.dumps(responses))

    async def broadcast(self, receiver: Callable, signal: Signal,
            sender: str, message: dict):

        self.log.debug('Calling %s by %s:%s with %s', receiver.__qualname__,
                        signal.name, sender, str(message).encode('utf-8'))

        try:
            response = receiver(signal=signal, sender=sender, **message)
        except TypeError:
            self.log.error('Call %s failed', signal.name, exc_info=True)
            return

        if asyncio.iscoroutine(response):
            timer = datetime.now().timestamp()

            try:
                response = await asyncio.wait_for(response, self.HANDLING_TIMEOUT)
                if isinstance(response, (int, str)):
                    return response

            except asyncio.TimeoutError:
                self.log.fatal(
                    'TimeoutError: %s %.2f', receiver.__qualname__,
                    datetime.now().timestamp() - timer)


class BoundSignal:
    __slots__ = ('bus', 'signal')

    def __init__(self, bus: AbstractSignalBus, signal: Signal):
        self.bus = bus
        self.signal = signal

    async def send(self, sender, **kwargs):
        await self.bus.send(
            self.signal.get_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs))

    async def call(self, sender, **kwargs):
        return await self.bus.call(
            self.signal.get_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs))
