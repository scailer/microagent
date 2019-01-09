import asyncio
import unittest
import pulsar
from pulsar.apps.data import create_store
from pulsar.apps.data.redis import RedisServer
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


class AgentTestCase(unittest.TestCase):
    CHANNEL_PREFIX = 'TEST'
    REDIS_DSN = 'redis://localhost:6379/5'
    AGENT_CLASS = None
    SETTINGS = {'redis_server': RedisServer(), 'signal_prefix': SignalPrefix()}

    @classmethod
    async def setUpClass(cls):
        cls.SETTINGS['redis_server'].set(cls.REDIS_DSN)
        cls.SETTINGS['signal_prefix'].set(cls.CHANNEL_PREFIX)
        cls.loop = asyncio.get_event_loop()
        cls.bus = RedisSignalBus(cls.REDIS_DSN, prefix=cls.CHANNEL_PREFIX)
        cls.agent = cls.AGENT_CLASS(cls.bus, settings=cls.SETTINGS)
