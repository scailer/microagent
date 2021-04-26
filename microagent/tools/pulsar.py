import ssl
import asyncio
import logging
from typing import Optional

from pulsar.apps.data import create_store

from ..bus import AbstractSignalBus
from ..broker import AbstractQueueBroker
from .redis import RedisBrokerMixin

try:
    from pulsar import Setting, Config, Application  # Ver 1.x
except ImportError:
    from pulsar.api import Setting, Config, Application  # Ver 2.x


class MicroAgentSetting(Setting):
    virtual = True
    app = 'microagent'
    section = 'Micro Agent'


class SignalPrefix(MicroAgentSetting):
    name = 'signal_prefix'
    flags = ['--signal-prefix']
    default = 'PUBSUB'
    desc = ''


class SignalBus(MicroAgentSetting):
    is_global = True
    name = 'signal_bus'
    flags = ['--signal-bus']
    meta = "CONNECTION_STRING"
    default = 'redis://127.0.0.1:6379/7'
    desc = 'DSN signal bus'


class QueueBroker(MicroAgentSetting):
    is_global = True
    name = 'queue_broker'
    flags = ['--queue-broker']
    meta = "CONNECTION_STRING"
    default = ''
    desc = 'DSN queue broker'


class BrokerSSLCert(MicroAgentSetting):
    is_global = True
    name = 'broker_cert'
    flags = ['--broker-cert']
    meta = "FILE"
    default = ''
    desc = 'Broker ssl certificate file'


class RedisSignalBus(AbstractSignalBus):
    def __init__(self, dsn, prefix='PUBSUB', logger=None):
        super().__init__(dsn, prefix, logger)
        _loop = asyncio.get_running_loop()
        redis = create_store(dsn, decode_responses=True, loop=_loop)
        self.transport = redis.pubsub()
        self.transport.add_client(self.receiver)

    async def send(self, channel, message):
        await self.transport.publish(channel, message)

    async def bind(self, channel):
        await self.transport.psubscribe(channel)

    def receiver(self, channel, message):
        self._receiver(channel, message)


class PulsarRedisBroker(RedisBrokerMixin, AbstractQueueBroker):
    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        super().__init__(dsn, logger)
        _loop = asyncio.get_running_loop()
        self.redis_store = create_store(dsn, decode_responses=True, loop=_loop)
        self.transport = self.redis_store.client()

    async def new_connection(self):
        return self.redis_store.client()

    async def send(self, name: str, message: str):
        await self.transport.rpush(name, message)

    async def queue_length(self, name: str):
        return int(await self.transport.llen(name))


class MicroAgentApp(Application):
    cfg = Config(apps=['microagent'])

    def worker_start(self, worker, exc=None):
        log = self.cfg.configured_logger()

        signal_bus_dsn = self.cfg.settings['signal_bus'].value
        signal_prefix = self.cfg.settings['signal_prefix'].value

        if signal_bus_dsn.startswith('redis'):
            bus = RedisSignalBus(signal_bus_dsn, prefix=signal_prefix, logger=log)

        elif signal_bus_dsn.startswith('aioredis'):
            from .aioredis import AIORedisSignalBus
            bus = AIORedisSignalBus(signal_bus_dsn[3:], prefix=signal_prefix, logger=log)

        else:
            bus = None

        queue_broker_dsn = self.cfg.settings.get('queue_broker').value

        if queue_broker_dsn.startswith('amqp'):
            from .amqp import AMQPBroker

            ssl_context, cert_file = None, self.cfg.settings.get('broker_cert').value

            if cert_file:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.load_verify_locations(cert_file)

            broker = AMQPBroker(queue_broker_dsn, ssl_context=ssl_context)

        elif queue_broker_dsn.startswith('kafka'):
            from .kafka import KafkaBroker
            broker = KafkaBroker(queue_broker_dsn)

        elif queue_broker_dsn.startswith('redis'):
            broker = PulsarRedisBroker(queue_broker_dsn)

        else:
            broker = None

        worker.agent = self.cfg.agent(
            bus=bus, broker=broker, logger=log, settings=self.cfg.settings)
        asyncio.ensure_future(worker.agent.start())

    def worker_stopping(self, worker, exc=None):
        asyncio.ensure_future(worker.agent.stop())
