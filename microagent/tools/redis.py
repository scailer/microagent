'''
:ref:`Signal Bus <bus>` and :ref:`Queue Broker <broker>` based on :redis:`redis <>`.
'''
import asyncio
import logging

from collections import defaultdict
from datetime import datetime
from typing import Any

from redis.asyncio import ConnectionError, Redis, client

from ..broker import AbstractQueueBroker
from ..bus import AbstractSignalBus


class AIORedisSignalBus(AbstractSignalBus):
    '''
        Bus is based on redis publish and subscribe features.
        Channel name is forming by rule ```{prefix}:{signal_name}:{sender_name}#{message_id}```

        Example:

        .. code-block:: python

            from microagent.tools.redis import AIORedisSignalBus

            bus = AIORedisSignalBus('redis://localhost/7', prefix='MYAPP', logger=custom_logger)

            await bus.user_created.send('user_agent', user_id=1)


    '''
    connection: Redis

    def __init__(self, dsn: str, prefix: str = 'PUBSUB',
            logger: logging.Logger | None = None) -> None:

        super().__init__(dsn, prefix, logger)
        self._pubsub_lock = asyncio.Lock()
        self.connection = self.new_connection()

    def new_connection(self) -> Redis:
        return Redis.from_url(self.dsn, decode_responses=True)

    async def send(self, channel: str, message: str) -> None:
        await self.connection.publish(channel, message)

    async def bind(self, channel: str) -> None:
        async with self._pubsub_lock:
            pubsub = self.connection.pubsub()
            asyncio.create_task(self._receiver(pubsub, channel))

    async def _receiver(self, pubsub: client.PubSub, channel: str) -> None:
        async with pubsub as psub:
            await pubsub.psubscribe(channel)

            try:
                async for message in psub.listen():
                    if message['type'] in psub.PUBLISH_MESSAGE_TYPES:
                        self.receiver(message['channel'], message['data'])

            except ConnectionError as exc:
                self.log.exception(exc)
                self.log.warning('Resubscribe %s %s', channel, self)
                self.connection = self.new_connection()
                await asyncio.sleep(1)
                await self.bind(channel)


class AIORedisBroker(AbstractQueueBroker):
    '''
        Broker is based on Redis lists and RPUSH and BLPOP commands.
        Queue name using as a key. If hanling faild, message will be returned
        to queue 3 times (by default) and then droped.

        Example:

        .. code-block:: python

            from microagent.tools.redis import AIORedisBroker

            broker = AIORedisBroker('redis://localhost/7', logger=custom_logger)

            await broker.user_created.send({'user_id': 1})

        .. attribute:: ROLLBACK_ATTEMPTS

            Number attempts for handling of message before it will be droped (by default: 3)

        .. attribute:: WAIT_TIME

            BLPOP option (by default: 15)
    '''
    WAIT_TIME: int = 15
    BIND_TIME: float = 1
    ROLLBACK_ATTEMPTS: int = 3

    connection: Redis
    log: logging.Logger
    _bindings: dict
    _rollbacks: dict

    def __init__(self, dsn: str, logger: logging.Logger | None = None) -> None:
        super().__init__(dsn, logger)  # type: ignore
        self.connection = self.new_connection()
        self._rollbacks = defaultdict(lambda: 0)

    def new_connection(self) -> Redis:
        return Redis.from_url(self.dsn, decode_responses=True)

    async def send(self, name: str, message: str, **kwargs: Any) -> None:
        await self.connection.rpush(name, message)  # type: ignore

    async def queue_length(self, name: str, **options: Any) -> int:
        return int(await self.connection.llen(name))  # type: ignore

    async def bind(self, name: str) -> None:
        _loop = asyncio.get_running_loop()
        _loop.call_later(self.BIND_TIME, lambda: asyncio.create_task(self._wait(name)))

    async def _wait(self, name: str) -> None:
        conn = await self.new_connection()
        while True:
            data = await conn.blpop(name, self.WAIT_TIME)
            if data:
                _, data = data
                asyncio.create_task(self._handler(name, data))

    async def rollback(self, name: str, data: str) -> None:
        _hash = str(hash(name)) + str(hash(data))
        attempt = self._rollbacks[_hash]

        if attempt > self.ROLLBACK_ATTEMPTS:
            self.log.error('Rollback limit exceeded on queue "%s" with data: %s', name, data)
            return

        self.log.warning('Back message to queue "%s" attempt %d', name, attempt)

        _loop = asyncio.get_running_loop()
        _loop.call_later(attempt ** 2, lambda: asyncio.create_task(self.send(name, data)))

        self._rollbacks[_hash] += 1

    async def _handler(self, name: str, data: str) -> None:
        consumer = self._bindings[name]
        _data = self.prepared_data(consumer, data)

        try:
            response = consumer.handler(**_data)
        except Exception:
            self.log.exception('Call %s failed', consumer.queue.name)
            await self.rollback(consumer.queue.name, data)
            return

        if asyncio.iscoroutine(response):
            timer = datetime.now().timestamp()

            try:
                response = await asyncio.wait_for(response, consumer.timeout)
            except asyncio.TimeoutError:
                self.log.error('TimeoutError: %s %.2f', consumer,
                    datetime.now().timestamp() - timer)
                await self.rollback(name, data)
            except Exception:
                self.log.exception('Call %s failed', consumer.queue.name)
                await self.rollback(consumer.queue.name, data)
