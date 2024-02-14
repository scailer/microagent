'''
Event-driven architecture is based on objects exchanging a non-directed messages - events.
Here we assume that events (signals) that are not stored anywhere, everyone can
receive and send them, like a radio-transfer.

Here, the intermediary that manages messages routing is called the Signal Bus and
implements the publish / subscribe pattern. A signal is a message with a strict fixed structure.
The Bus contains many channels that are different for each type of signal.
We can send a signal from many sources and listen it with many receivers.

Implementations:

* :ref:`redis <tools-redis>`


Using SignalBus separately (sending only)

.. code-block:: python

    from microagent import load_signals
    from microagent.tools.redis import RedisSignalBus

    signals = load_signals('file://signals.json')

    bus = RedisSignalBus('redis://localhost/7')
    await bus.user_created.send('user_agent', user_id=1)


Using with MicroAgent

.. code-block:: python

    from microagent import MicroAgent, load_signals
    from microagent.tools.redis import RedisSignalBus

    signals = load_signals('file://signals.json')

    class UserAgent(MicroAgent):
        @receiver(signals.user_created)
        async def example(self, user_id, **kwargs):
            await self.bus.user_created.send('some_signal', user_id=1)

    bus = RedisSignalBus('redis://localhost/7')
    user_agent = UserAgent(bus=bus)
    await user_agent.start()
'''
import asyncio
import contextlib
import logging
import time
import uuid

from abc import abstractmethod
from collections import abc, defaultdict
from dataclasses import dataclass, field
from typing import Any

from .abc import BusProtocol, SignalProtocol
from .signal import Receiver, SerializingError, Signal
from .utils import IterQueue, raise_timeout


@dataclass(slots=True)
class AbstractSignalBus(BusProtocol):
    '''
        Signal bus is an abstract interface with two basic methods - send and bind.

        `send`-method allows to publish some signal in the channel for subscribers.

        `bind`-method allows to subscribe to the channel(s) for receive the signal(s).

        `call`-method allows to use RPC based on `send` and `bind`.

        All registered Signals are available in the bus object as attributes
        with the names specified in the declaration.

        .. code-block:: python

            Signal(name='user_created', providing_args=['user_id'])

            bus = RedisSignalBus('redis://localhost/7')
            await bus.user_created.send('user_agent', user_id=1)

        .. attribute:: dsn

            Bus has only one required parameter - dsn-string (data source name),
            which provide details for establishing a connection with the mediator-service.

        .. attribute:: prefix

            Channel prefix, string, one for project. It is allowing use same
            redis for different projects.

        .. attribute:: log

            Provided or defaul logger

        .. attribute:: uid

            UUID, id of bus instance (for debugging)

        .. attribute:: receivers

            Dict of all binded receivers
    '''

    dsn: str
    uid: str = field(default_factory=lambda: uuid.uuid4().hex)
    prefix: str = 'PUBSUB'
    log: logging.Logger = logging.getLogger('microagent.bus')

    receivers: dict[str, list[Receiver]] = field(default_factory=lambda: defaultdict(list))
    _responses: dict[str, IterQueue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        response_signal = Signal(name='response', providing_args=[])
        asyncio.create_task(self.bind(response_signal.make_channel_name(self.prefix)))
        self.log.debug('%s initialized', self)

    def __repr__(self) -> str:
        return f'<Bus {self.__class__.__name__} {self.dsn} {self.prefix}#{self.uid}>'

    def __getattr__(self, name: str) -> 'BoundSignal':
        signal = Signal.get(name)
        return BoundSignal(self, signal)

    @abstractmethod
    async def send(self, channel: str, message: str) -> None:
        '''
            Send raw message to channel.
            Available optional type checking for input data.

            :param channel: string, channel name
            :param message: string, serialized object
        '''
        ...

    @abstractmethod
    async def bind(self, signal: str) -> None:
        '''
            Subscribe to channel.

            :param signal: string, signal name for subscribe
        '''
        ...

    async def bind_receiver(self, receiver: Receiver) -> None:
        '''
            Bind bounded to agent receiver to current bus.
        '''
        self.log.info('Bind %s to %s: %s', receiver.signal, self, receiver)
        if receiver.signal.name not in self.receivers:
            await self.bind(receiver.signal.make_channel_name(self.prefix))
        self.receivers[receiver.signal.name].append(receiver)

    @contextlib.asynccontextmanager
    async def call(self, channel: str, message: str, timeout: int) -> abc.AsyncIterator[IterQueue]:
        '''
            RPC over pub/sub. Pair of signals - sending and responsing. Response-signal
            is an internal construction enabled by default. When we call `call` we send
            a ordinary declared by user signal with a unique id and awaiting a response
            with same id. The response can contain a string value or an integer that is
            returned by the signal receiver.

            Signal-attached method `call` will catch only first value.
            To process multiple responses, you can use async context `waiter`, which
            will return an async generator of response data.
            You can break it or return value when it needed. `waiter`-method has the
            `timeout` argument set to 60 by default.

            Answer: ```{"<Class>.<method>": <value>}```

            Available optional type checking for input data.

            .. code-block:: python

                class CommentAgent(MicroAgent):
                    @receiver(signals.rpc_comments_count)
                    async def example_rpc_handler(self, user_id, **kwargs):
                        return 1

                response = await bus.rpc_comments_count.call('user_agent', user_id=1)
                value = response['CommentAgent.example_rpc_handler']

                async with bus.rpc_comments_count.waiter('user_agent', user_id=1) as queue:
                    async for x in queue:
                        logging.info('Get response %s', x)
                        break
        '''

        queue: IterQueue = IterQueue()
        request_id = uuid.uuid4().hex
        self._responses[request_id] = queue

        await self.send(f'{channel}#{request_id}', message)

        try:
            raise_timeout(timeout)
            yield queue
        finally:
            self._responses.pop(request_id, None)

    def receiver(self, channel: str, message: str) -> None:
        '''
            Handler of raw incoming messages.
            Available optional type checking for input data.
        '''

        signal_id: str | None = None

        if '#' in channel:
            channel, signal_id = channel.split('#')

        _, name, sender = channel.split(':')  # prefix:name:sender
        signal = Signal.get(name)

        try:
            data = signal.deserialize(message)  # type: dict
        except SerializingError:
            self.log.error('Invalid pubsub message: %s', message)
            return None

        if name == 'response' and signal_id:
            return self.handle_response(signal_id, data)

        if signal.type_map:
            check_types(signal, data, self.log)

        diff_args = set(signal.providing_args) ^ set(data.keys())

        if diff_args:
            self.log.warning('Pubsub mismatch arguments %s %s', channel, diff_args)

        asyncio.create_task(self.handle_signal(signal, sender, signal_id, data))

        return None

    def handle_response(self, signal_id: str, message: dict[str, int | str | None]) -> None:
        if queue := self._responses.get(signal_id):
            queue.put_nowait(message)

    async def handle_signal(self, signal: Signal, sender: str,
            signal_id: str | None, message: dict) -> None:

        receivers: list[Receiver] = self.receivers.get(signal.name, [])

        responses: list[int | str | None] = await asyncio.gather(*[
            self.broadcast(receiver, signal, sender, message)
            for receiver in receivers
        ])

        if signal_id:
            await self.send(
                f'{self.prefix}:response:{self.uid}#{signal_id}',
                Signal._jsonlib.dumps({
                    rec.key: res for rec, res in zip(receivers, responses, strict=True)
                })
            )

    async def broadcast(self, receiver: Receiver, signal: Signal,
            sender: str, message: dict) -> int | str | None:

        self.log.debug('Calling %s by %s:%s with %s', receiver.handler,
            signal.name, sender, str(message).encode('utf-8'))

        timer = time.monotonic()  # type: float

        try:
            response = await asyncio.wait_for(
                receiver.handler(signal=signal, sender=sender, **message),
                receiver.timeout
            )
            if isinstance(response, int | str):
                return response

        except TypeError:
            self.log.exception('Call %s failed', signal.name)

        except asyncio.TimeoutError:
            self.log.error(
                'TimeoutError: %s %.2f', receiver.handler, time.monotonic() - timer)

        return None


@dataclass(slots=True, frozen=True)
class BoundSignal(SignalProtocol):
    bus: AbstractSignalBus
    signal: Signal

    async def send(self, sender: str, **kwargs: Any) -> None:
        if self.signal.type_map:
            check_types(self.signal, kwargs, self.bus.log)

        await self.bus.send(
            self.signal.make_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs))

    async def call(self, sender: str, timeout: int = 60, **kwargs: Any
            ) -> dict[str, int | str | None]:

        if self.signal.type_map:
            check_types(self.signal, kwargs, self.bus.log)

        gen = self.bus.call(
            self.signal.make_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs),
            timeout=timeout
        )

        async with gen as queue:
            async for value in queue:
                return value

        return {}

    def waiter(self, sender: str, timeout: int = 60, **kwargs: Any
                ) -> contextlib.AbstractAsyncContextManager:
        '''
            async with bus.iter(sender='name', a=1, timeout=10) as queue:
                async for x in queue:
                    logging.info('Get response %s', x)
                    break
        '''
        return self.bus.call(
            self.signal.make_channel_name(self.bus.prefix, sender),
            self.signal.serialize(kwargs),
            timeout=timeout
        )


def check_types(signal: Signal, data: dict, log: logging.Logger) -> None:
    if signal.type_map:
        for key, value in data.items():
            if key not in signal.type_map:
                log.warning('Receiver get unknown arg "%s" %s', key, value)
            elif not isinstance(value, signal.type_map[key]):
                log.warning('Receiver get wrong type for "%s" %s', key, value)
