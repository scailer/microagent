'''
:ref:`Queue Broker <broker>` based on :aioamqp:`aioamqp <>`.
'''
import logging
import asyncio

from functools import partial
from dataclasses import dataclass
from collections import defaultdict
from typing import Optional, Dict, Any, Callable
from datetime import datetime

import aioamqp  # type: ignore
from ..broker import AbstractQueueBroker, Consumer, Queue

try:
    from ssl import SSLContext
except ImportError:  # pragma: no cover
    SSLContext = Any  # type: ignore


@dataclass(frozen=True)
class MessageMeta:
    '''
        .. _amqp_meta:

        MessageMeta - DTO for entity provided by aioamqp

        .. attribute:: queue

            Queue object

        .. attribute:: channel

            aioamqp.channel.Channel

        .. attribute:: envelope

            aioamqp.envelope.Envelope

        .. attribute:: properties

            aioamqp.properties.Properties
    '''
    queue: Queue
    channel: aioamqp.channel.Channel
    envelope: aioamqp.envelope.Envelope
    properties: aioamqp.properties.Properties


class ChannelContext:
    broker: 'AMQPBroker'
    channel: Optional[aioamqp.channel.Channel]

    def __init__(self, broker: 'AMQPBroker'):
        self.broker = broker
        self.channel = None

    async def __aenter__(self) -> aioamqp.channel.Channel:
        self.channel = await self.broker.get_channel()
        return self.channel

    async def __aexit__(self, exc_type, exc, traceback):
        await self.channel.close()
        return True


class AMQPBroker(AbstractQueueBroker):
    '''
        The broker is based on the basic_consume method of the AMQP and sends
        a acknowledgement automatically if the handler is completed without errors.
        The consumer takes an exclusive channel. Sending an reuse the channels.

        :param dsn: string, data source name for connection amqp://guest@localhost:5672/
        :param ssl_context: SSLContext object for encrypted amqps connection (only for amqps)
        :param logger: logging.Logger (optional)

        .. code-block:: python

            from microagent.tools.amqp import AMQPBroker

            broker = AMQPBroker('amqp://guest:guest@localhost:5672/')

            await broker.user_created.send({'user_id': 1})


        `@consumer`-decorator for this broker has an additional option - `autoack`,
        which enables / disables sending automatic acknowledgements.

        .. code-block:: python

            class EmailAgent(MicroAgent):
                @consumer(queues.mailer, autoack=False)
                async def example_read_queue(self, amqp, **data):
                    if amqp.channel.is_open:
                        await amqp.channel.basic_client_ack(delivery_tag=amqp.envelope.delivery_tag)
                    else:
                        channel = await self.broker.get_channel()
                        await channel.basic_client_ack(delivery_tag=amqp.envelope.delivery_tag)


        Handler will takes one required positional argument - :ref:`MessageMeta <amqp_meta>`.
        Consumer will be reconnect and subscribe to queue on disconnect.
        It make 3 attempts of reconnect after 1, 4, 9 seconds.
        if the queue does not exist, it will be declared with the default parameters when binding.

    '''
    REBIND_ATTEMPTS = 3

    protocol: Optional[aioamqp.AmqpProtocol]
    ssl_context: Optional[SSLContext]
    _bind_attempts: dict

    def __init__(self, dsn: str, ssl_context: SSLContext = None, logger: logging.Logger = None):
        super().__init__(dsn, logger)

        self.protocol = None
        self._bind_attempts = defaultdict(lambda: 1)
        self.ssl_context = ssl_context

    @property
    def channels(self) -> Dict[int, aioamqp.channel.Channel]:
        ''' Dict of open channels '''
        if self.protocol:
            return self.protocol.channels
        else:
            return {}

    async def connect(self, **kwargs) -> aioamqp.AmqpProtocol:
        url = aioamqp.urlparse(self.dsn)
        return await aioamqp.connect(
            host=url.hostname or 'localhost',
            port=url.port,
            login=url.username or 'guest',
            password=url.password or 'guest',
            virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
            ssl=self.ssl_context if url.scheme == 'amqps' else None,
            **kwargs
        )

    async def send(self, name: str, message: str, exchange: str = '', **kwargs) -> None:
        '''
            Raw message sending.

            :param name: string, target queue name (routing_key)
            :param message: string, serialized message
            :param exchange: string, target exchange name
            :param \*\*kwargs: dict, other basic_publish options
        '''  # noqa: W605
        async with ChannelContext(self) as channel:
            await channel.basic_publish(message, routing_key=name, exchange_name=exchange, **kwargs)

    def _on_amqp_error(self, name: str, exception: Exception):
        self.log.warning('Catch AMPQ exception %s on queue "%s"', exception, name)
        handler = self._bindings.pop(name, None)

        if not handler:
            self.log.error('Failed rebind queue "%s" without handler', name)
            return

        asyncio.create_task(self.rebind(name))

    async def rebind(self, name: str) -> None:
        if self._bind_attempts[name] > self.REBIND_ATTEMPTS:
            self.log.error('Failed all attempts to rebind queue "%s"', name, exc_info=True)
            return

        await asyncio.sleep(self._bind_attempts[name] ** 2)
        self._bind_attempts[name] += 1

        try:
            await self.bind(name)
            self.log.info('Success rebind queue "%s"', name)
            del self._bind_attempts[name]
            return True

        except (OSError, aioamqp.AmqpClosedConnection, aioamqp.ChannelClosed) as exc:
            self.log.error('Failed rebind queue "%s": %s', name, exc, exc_info=True)
            asyncio.create_task(self.rebind(name))
            return False

    async def bind(self, name: str):
        _, protocol = await self.connect(on_error=partial(self._on_amqp_error, name))
        channel = await protocol.channel()  # type: aioamqp.channel.Channel
        consumer = self._bindings[name]

        try:
            await channel.basic_consume(self._amqp_wrapper(consumer), queue_name=name)

        except aioamqp.ChannelClosed as exc:
            if exc.code != 404:
                raise

            self.log.warning('Declare queue "%s"', name)
            channel = await protocol.channel()  # type: ignore
            await channel.queue_declare(name)
            await channel.basic_consume(self._amqp_wrapper(consumer), queue_name=name)

    async def get_channel(self) -> aioamqp.channel.Channel:
        '''
            Takes a channel from the pool or a new one, performs a lazy connection if required.
        '''
        if not self.protocol:
            _, self.protocol = await self.connect()

        try:
            return await self.protocol.channel()
        except aioamqp.AmqpClosedConnection:
            self.protocol = None  # Drop connection cache
            raise

    def _amqp_wrapper(self, consumer: Consumer) -> Callable:
        async def _wrapper(channel, body, envelope, properties):
            data = consumer.queue.deserialize(body)

            if not data:
                self.log.debug('Calling %s by %s without data', consumer, consumer.queue.name)
                return

            data['amqp'] = MessageMeta(
                queue=consumer.queue, channel=channel,
                envelope=envelope, properties=properties)

            self.log.debug('Calling %s by %s with %s', consumer,
                consumer.queue.name, str(data).encode('utf-8'))

            try:
                response = consumer.handler(**data)
            except TypeError:
                self.log.error('Call %s failed', consumer, exc_info=True)
                return

            if asyncio.iscoroutine(response):
                timer = datetime.now().timestamp()

                try:
                    await asyncio.wait_for(response, consumer.timeout)
                except asyncio.TimeoutError:
                    self.log.fatal('TimeoutError: %s %.2f', consumer,
                        datetime.now().timestamp() - timer)
                    return

            if consumer.options.get('autoack', True):
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        return _wrapper

    async def declare_queue(self, name: str, **options) -> None:
        '''
            Declare queue with queue_declare method.

            :param name: string, queue name
            :param \*\*options: other queue_declare options
        '''  # noqa: W605
        async with ChannelContext(self) as channel:
            info = await channel.queue_declare(name, **options)
            self.log.info('Declare/get queue "%(queue)s" with %(message_count)s '
                'messages, %(consumer_count)s consumers', info)

    async def queue_length(self, name: str) -> int:
        '''
            Get a queue length with queue_declare method.

            :param name: string, queue name
        '''
        async with ChannelContext(self) as channel:
            info = await channel.queue_declare(name)
            return int(info['message_count'])

    async def putout(self, amqp: MessageMeta) -> None:
        '''
            Send acknowledgement to broker with basic_client_ack

            :param amqp: MessageMeta
        '''
        if amqp.channel.is_open:
            await amqp.channel.basic_client_ack(delivery_tag=amqp.envelope.delivery_tag)
        else:
            channel = await self.get_channel()
            await channel.basic_client_ack(delivery_tag=amqp.envelope.delivery_tag)
