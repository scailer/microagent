'''
Event-driven architecture is based on objects exchanging a non-directed messages - events.
Here we assume that events (signals) that are not stored anywhere, everyone can
receive and send them, like a radio-transfer.

Here, the intermediary that manages messages routing is called the Signal Bus and
implements the publish / subscribe pattern. A signal is a message with a strict fixed structure.
The Bus contains many channels that are different for each type of signal.
We can send a signal from many sources and listen it with many receivers.

Implementations:

* :ref:`aioredis <tools-aioredis>`


Using SignalBus separately (sending only)

.. code-block:: python

    from microagent import load_signals
    from microagent.tools.aioredis import AIORedisSignalBus

    signals = load_signals('file://signals.json')

    bus = AIORedisSignalBus('redis://localhost/7')
    await bus.user_created.send('user_agent', user_id=1)


Using with MicroAgent

.. code-block:: python

    from microagent import MicroAgent, load_signals
    from microagent.tools.aioredis import AIORedisSignalBus

    signals = load_signals('file://signals.json')

    class UserAgent(MicroAgent):
        @receiver(signals.user_created)
        async def example(self, user_id, **kwargs):
            await self.bus.user_created.send('some_signal', user_id=1)

    bus = AIORedisSignalBus('redis://localhost/7')
    user_agent = UserAgent(bus=bus)
    await user_agent.start()
'''
import abc
import uuid
import logging
import asyncio
import inspect
import ujson

from collections import defaultdict
from typing import Optional, List, Union, Dict, Any, Tuple, Type
from datetime import datetime

from .signal import Signal, Receiver, SerializingError


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

    def complete(self) -> None:
        if self.await_from:
            result = {k: v for k, v in self.response.items() if k in self.await_from}
        else:
            result = self.response

        self.fut.set_result(result)
        self.close()

    @classmethod
    def get(cls, signal_id: str) -> Optional['ResponseContext']:
        return cls._responses.get(signal_id)

    @classmethod
    def finish(cls, signal_id: str, message: Dict[str, Union[str, int, None]]) -> None:
        resp = cls.get(signal_id)  # type: Optional[ResponseContext]

        if not resp:  # already closed
            return None

        resp.response.update(message)

        if not resp.await_from:  # return first
            resp.complete()

        elif not (set(resp.await_from) - set(x.split('.')[0] for x in resp.response.keys())):
            resp.complete()

        else:
            return None


def response_context_factory() -> Type[ResponseContext]:
    '''
        Make isolated response context

    '''

    class BoundResponseContext(ResponseContext):
        _responses: dict = {}

    return BoundResponseContext


class AbstractSignalBus(abc.ABC):
    '''
        Signal bus is an abstract interface with two basic methods - send and bind.

        `send`-method allows to publish some signal in the channel for subscribers.

        `bind`-method allows to subscribe to the channel(s) for receive the signal(s).

        `call`-method allows to use RPC based on `send` and `bind`.

        All registered Signals are available in the bus object as attributes
        with the names specified in the declaration.

        .. code-block:: python

            Signal(name='user_created', providing_args=['user_id'])

            bus = AIORedisSignalBus('redis://localhost/7')
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

        response_signal = Signal(name='response', providing_args=[])
        asyncio.create_task(self.bind(response_signal.make_channel_name(self.prefix)))

        self.log.debug('%s initialized', self)

    def __repr__(self) -> str:
        return f'<Bus {self.__class__.__name__} {self.dsn} {self.prefix}#{self.uid}>'

    def __getattr__(self, name: str) -> 'BoundSignal':
        signal = Signal.get(name)
        return BoundSignal(self, signal)

    @abc.abstractmethod
    def send(self, channel: str, message: str):
        '''
            Send raw message to channel.

            :param channel: string, channel name
            :param message: string, serialized object
        '''
        return NotImplemented  # pragma: no cover

    @abc.abstractmethod
    def bind(self, signal: str):
        '''
            Subscribe to channel.

            :param signal: string, signal name for subscribe
        '''
        return NotImplemented  # pragma: no cover

    async def bind_receiver(self, receiver: Receiver) -> None:
        '''
            Bind bounded to agent receiver to current bus.
        '''
        self.log.info('Bind %s to %s: %s', receiver.signal, self, receiver)
        if receiver.signal.name not in self.receivers:
            await self.bind(receiver.signal.make_channel_name(self.prefix))
        self.receivers[receiver.signal.name].append(receiver)

    async def call(self, channel: str, message: str, await_from: List[str] = None
                ) -> Dict[str, Union[int, str, None]]:
        '''
            RPC over pub/sub. Pair of signals - sending and responsing. Response-signal
            is an internal construction enabled by default. When we call `call` we send
            a ordinary declared by user signal with a unique id and awaiting a response
            with same id. The response can contain a string value or an integer that is
            returned by the signal receiver. By default, it will catch only first value
            if we have multiple signal receivers, but we can specify the name of the
            target receiver that will respond to us.

            Answer: ```{"<Class>.<method>": <value>}```

            .. code-block:: python

                class CommentAgent(MicroAgent):
                    @receiver(signals.rpc_comments_count)
                    async def example_rpc_handler(self, user_id, **kwargs):
                        return 1

                response = await bus.rpc_comments_count.call('user_agent', user_id=1)
                value = response['CommentAgent.example_rpc_handler']
        '''
        async with self.response_context(await_from, self.RESPONSE_TIMEOUT) as (signal_id, future):
            await self.send(f'{channel}#{signal_id}', message)
            return await future

    def receiver(self, channel: str, message: str) -> None:
        '''
            Handler of raw incoming messages.
        '''

        signal_id = None  # type: Optional[str]

        if '#' in channel:
            channel, signal_id = channel.split('#')

        pref, name, sender = channel.split(':')
        signal = Signal.get(name)  # type: Signal

        try:
            data = signal.deserialize(message)  # type: dict
        except SerializingError:
            self.log.error('Invalid pubsub message: %s', message)
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
            self.log.exception('Response handle failed: %s', exc)

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
                    rec.key: res for rec, res in zip(receivers, responses)
                })
            )

    async def broadcast(self, receiver: Receiver, signal: Signal,
            sender: str, message: dict) -> Union[int, str, None]:

        self.log.debug('Calling %s by %s:%s with %s', receiver.handler,
            signal.name, sender, str(message).encode('utf-8'))

        try:
            response = receiver.handler(signal=signal, sender=sender, **message)
        except TypeError:
            self.log.exception('Call %s failed', signal.name)
            return None

        if inspect.isawaitable(response):
            timer = datetime.now().timestamp()  # type: float

            try:
                response = await asyncio.wait_for(response, receiver.timeout)

            except asyncio.TimeoutError:
                self.log.error(
                    'TimeoutError: %s %.2f', receiver.handler,
                    datetime.now().timestamp() - timer)
                return None

        if isinstance(response, (int, str)):
            return response


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
