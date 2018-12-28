import asyncio
import pulsar
from pulsar.apps.data import create_store
from ..bus import AbstractSignalBus


class MicroAgentSetting(pulsar.Setting):
    virtual = True
    app = 'microagent'
    section = 'Micro Agent'


class SignalPrefix(MicroAgentSetting):
    name = 'signal_prefix'
    flags = ['--signal-prefix']
    default = 'PUBSUB'
    desc = ''


class RedisSignalBus(AbstractSignalBus):
    def __init__(self, dsn, prefix='PUBSUB', logger=None):
        super().__init__(dsn, prefix, logger)
        redis = create_store(dsn, decode_responses=True, loop=self._loop)
        self.transport = redis.pubsub()
        self.transport.add_client(self.receiver)

    async def send(self, channel, message):
        await self.transport.publish(channel, message)

    async def bind(self, channel):
        await self.transport.psubscribe(channel)

    def receiver(self, channel, message):
        self._receiver(channel, message)


class MicroAgentApp(pulsar.Application):
    cfg = pulsar.Config(apps=['microagent'])

    def worker_start(self, worker, exc=None):
        log = self.cfg.configured_logger()
        redis_dsn = self.cfg.settings.get('redis_server').value
        signal_prefix = self.cfg.settings.get('signal_prefix').value
        bus = RedisSignalBus(redis_dsn, prefix=signal_prefix, logger=log)
        worker.agent = self.cfg.agent(bus, log, settings=self.cfg.settings)
