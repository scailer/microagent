'''
:ref:`Signal Bus <bus>` and :ref:`Queue Broker <broker>` based on :aioredis:`aioredis <>`.
'''
import functools
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
    mpsc: aioredis.pubsub.Receiver
    transport: Optional[aioredis.Redis]
    pubsub: Optional[aioredis.Redis]

    def __init__(self, dsn: str, prefix: str = 'PUBSUB', logger: logging.Logger = None) -> None:
        super().__init__(dsn, prefix, logger)
        self.mpsc = aioredis.pubsub.Receiver(loop=asyncio.get_running_loop())
        self.transport = None
        self.pubsub = None
        self._pubsub_lock = asyncio.Lock()  # type: asyncio.Lock
        asyncio.ensure_future(self._receiver(self.mpsc))

    async def send(self, channel: str, message: str) -> None:
        if not self.transport:
            self.transport = await aioredis.create_redis(self.dsn)
        await self.transport.publish(channel, message)

    async def bind(self, channel: str) -> None:
        async with self._pubsub_lock:
            if not self.pubsub:
                self.pubsub = await aioredis.create_redis(self.dsn)
            await self.pubsub.psubscribe(self.mpsc.pattern(channel))

    async def _receiver(self, mpsc: aioredis.pubsub.Receiver) -> None:
        async for chl, msg in mpsc.iter():
            channel, message = map(functools.partial(str, encoding='utf8'), msg)
            self.receiver(channel, message)


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

    async def new_connection(self) -> aioredis.Redis:
        return await aioredis.create_redis(self.dsn)

    async def send(self, name: str, message: str, **kwargs) -> None:
        if not self.transport:
            self.transport = await self.new_connection()
        await self.transport.rpush(name, message)  # type: ignore

    async def queue_length(self, name: str, **options) -> int:
        if not self.transport:
            self.transport = await self.new_connection()
        return int(await self.transport.llen(name))  # type: ignore
