'''
:ref:`Signal Bus <bus>` and :ref:`Queue Broker <broker>` based on :aioredis:`aioredis <>`.
'''
import asyncio
import logging
from typing import Optional

import aioredis  # type: ignore

from ..bus import AbstractSignalBus
from ..broker import AbstractQueueBroker
from .redis import RedisBrokerMixin


class AIORedisSignalBus(AbstractSignalBus):
    '''
        Bus is based on redis publish and subscribe features.
        Channel name is forming by rule ```{prefix}:{signal_name}:{sender_name}#{message_id}```

        Example:

        .. code-block:: python

            from microagent.tools.aioredis import AIORedisSignalBus

            bus = AIORedisSignalBus('redis://localhost/7', prefix='MYAPP', logger=custom_logger)

            await bus.user_created.send('user_agent', user_id=1)


    '''
    connection: Optional[aioredis.Redis] = None

    def __init__(self, dsn: str, prefix: str = 'PUBSUB', logger: logging.Logger = None) -> None:
        super().__init__(dsn, prefix, logger)
        self._pubsub_lock = asyncio.Lock()  # type: asyncio.Lock

    def get_connection(self):
        if not self.connection:
            self.connection = aioredis.Redis.from_url(self.dsn, decode_responses=True)
        return self.connection

    async def send(self, channel: str, message: str) -> None:
        await self.get_connection().publish(channel, message)

    async def bind(self, channel: str) -> None:
        async with self._pubsub_lock:
            pubsub = self.get_connection().pubsub()
            asyncio.create_task(self._receiver(pubsub, channel))

    async def _receiver(self, pubsub: aioredis.client.PubSub, channel: str) -> None:
        async with pubsub as psub:
            await pubsub.psubscribe(channel)

            try:
                async for message in psub.listen():
                    if message['type'] in psub.PUBLISH_MESSAGE_TYPES:
                        self.receiver(message['channel'], message['data'])

            except aioredis.ConnectionError as exc:
                self.log.exception(exc)
                self.log.warning('Resubscribe %s %s', channel, self)
                self.connection = None
                await asyncio.sleep(1)
                await self.bind(channel)


class AIORedisBroker(RedisBrokerMixin, AbstractQueueBroker):
    '''
        Broker is based on Redis lists and RPUSH and BLPOP commands.
        Queue name using as a key. If hanling faild, message will be returned
        to queue 3 times (by default) and then droped.

        Example:

        .. code-block:: python

            from microagent.tools.aioredis import AIORedisBroker

            broker = AIORedisBroker('redis://localhost/7', logger=custom_logger)

            await broker.user_created.send({'user_id': 1})

        .. attribute:: ROLLBACK_ATTEMPTS

            Number attempts for handling of message before it will be droped (by default: 3)

        .. attribute:: WAIT_TIME

            BLPOP option (by default: 15)
    '''

    def new_connection(self) -> aioredis.Redis:
        return aioredis.Redis.from_url(self.dsn, decode_responses=True)

    async def send(self, name: str, message: str, **kwargs) -> None:
        if not self.transport:
            self.transport = self.new_connection()
        await self.transport.rpush(name, message)  # type: ignore

    async def queue_length(self, name: str, **options) -> int:
        if not self.transport:
            self.transport = self.new_connection()
        return int(await self.transport.llen(name))  # type: ignore
