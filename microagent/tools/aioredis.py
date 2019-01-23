import functools
import asyncio
import aioredis

from ..bus import AbstractSignalBus


class AIORedisSignalBus(AbstractSignalBus):
    def __init__(self, dsn, prefix='PUBSUB', logger=None):
        super().__init__(dsn, prefix, logger)
        self.mpsc = aioredis.pubsub.Receiver(loop=self._loop)
        self.transport = None
        self.pubsub = None
        asyncio.ensure_future(self.receiver(self.mpsc))

    async def send(self, channel, message):
        if not self.transport:
            self.transport = await aioredis.create_redis(self.dsn)
        await self.transport.publish(channel, message)

    async def bind(self, channel):
        if not self.pubsub:
            self.pubsub = await aioredis.create_redis(self.dsn)
        await self.pubsub.psubscribe(self.mpsc.pattern(channel))

    async def receiver(self, mpsc):
        async for chl, msg in mpsc.iter():
            channel, message = map(functools.partial(str, encoding='utf8'), msg)
            self._receiver(channel, message)
