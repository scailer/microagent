import functools
import asyncio
import aioredis

from ..bus import AbstractSignalBus
from ..broker import AbstractQueueBroker
from .redis import RedisBrokerMixin


class AIORedisSignalBus(AbstractSignalBus):
    def __init__(self, dsn, prefix='PUBSUB', logger=None) -> None:
        super().__init__(dsn, prefix, logger)
        self.mpsc = aioredis.pubsub.Receiver(loop=asyncio.get_running_loop())
        self.transport = None
        self.pubsub = None
        self._pubsub_lock = asyncio.Lock()
        asyncio.ensure_future(self.receiver(self.mpsc))

    async def send(self, channel: str, message: str) -> None:
        if not self.transport:
            self.transport = await aioredis.create_redis(self.dsn)
        await self.transport.publish(channel, message)

    async def bind(self, channel: str) -> None:
        async with self._pubsub_lock:
            if not self.pubsub:
                self.pubsub = await aioredis.create_redis(self.dsn)
            await self.pubsub.psubscribe(self.mpsc.pattern(channel))

    async def receiver(self, mpsc: aioredis.pubsub.Receiver) -> None:
        async for chl, msg in mpsc.iter():
            channel, message = map(functools.partial(str, encoding='utf8'), msg)
            self._receiver(channel, message)


class AIORedisBroker(RedisBrokerMixin, AbstractQueueBroker):
    async def new_connection(self):
        return await aioredis.create_redis(self.dsn)

    async def send(self, name: str, message: str):
        if not self.transport:
            self.transport = await self.new_connection()
        await self.transport.rpush(name, message)

    async def queue_length(self, name: str):
        if not self.transport:
            self.transport = await self.new_connection()
        return int(await self.transport.llen(name))
