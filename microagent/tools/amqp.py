import logging
import asyncio

from collections import namedtuple, defaultdict
from functools import partial
from datetime import datetime
from typing import Optional

import aioamqp
from ..broker import AbstractQueueBroker


MessageMeta = namedtuple('MessageMeta', ['queue', 'channel', 'envelope', 'properties'])


class ChannelContext:
    def __init__(self, broker, once=False):
        self.broker = broker
        self.once = once

    async def __aenter__(self):
        try:
            channel_id = self.broker._reuse_channel_ids.pop()
            self.channel = self.broker.channels[channel_id]

        except KeyError:
            self.channel = await self.broker.get_channel()

        return self.channel

    async def __aexit__(self, exc_type, exc, traceback):
        if self.once:
            await self.channel.close()

        else:
            self.broker._reuse_channel_ids.add(self.channel.channel_id)

        return True


class AMQPBroker(AbstractQueueBroker):
    REBIND_ATTEMPTS = 3

    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        super().__init__(dsn, logger)
        self.protocol = None
        self._bind_attempts = defaultdict(lambda: 1)
        self._reuse_channel_ids = set()

    @property
    def channels(self):
        if self.protocol:
            return self.protocol.channels
        else:
            return {}

    async def send(self, name: str, message: str, **kwargs):
        kwargs['exchange_name'] = kwargs.get('exchange_name', '')
        async with ChannelContext(self) as channel:
            await channel.basic_publish(message, routing_key=name, **kwargs)

    def _on_amqp_error(self, name, exception):
        self.log.warning('Catch AMPQ exception %s on queue "%s"', exception, name)
        handler = self._bindings.pop(name, None)

        if not handler:
            self.log.error('Failed rebind queue "%s" without handler', name)
            return

        asyncio.ensure_future(self.rebind(name, handler))

    async def rebind(self, name, handler):
        if self._bind_attempts[name] > self.REBIND_ATTEMPTS:
            self.log.error('Failed all attempts to rebind queue "%s"', name, exc_info=True)
            return

        await asyncio.sleep(self._bind_attempts[name] ** 2)
        self._bind_attempts[name] += 1

        try:
            await self.bind(name, handler)
            self.log.info('Success rebind queue "%s": %s', name, handler)
            del self._bind_attempts[name]

        except (OSError, aioamqp.AmqpClosedConnection, aioamqp.ChannelClosed) as exc:
            self.log.error('Failed rebind queue "%s": %s', name, exc, exc_info=True)
            asyncio.ensure_future(self.rebind(name, handler))

    async def bind(self, name, handler):
        if name in self._bindings:
            self.log.warning('Handler to queue "%s" already binded. Ignoring', name)
            return

        _, protocol = await aioamqp.from_url(
            self.dsn, on_error=partial(self._on_amqp_error, name))
        channel = await protocol.channel()

        self.log.debug('Bind %s to queue "%s"', handler, name)

        try:
            await channel.basic_consume(self._amqp_wrapper(handler), queue_name=name)

        except aioamqp.ChannelClosed as exc:
            if exc.code != 404:
                raise

            self.log.warning('Declare queue "%s"', name)
            channel = await protocol.channel()
            await channel.queue_declare(name)
            await channel.basic_consume(self._amqp_wrapper(handler), queue_name=name)

        self._bindings[name] = handler

    async def get_channel(self):
        if not self.protocol:
            self._reuse_channel_ids = set()
            _, self.protocol = await aioamqp.from_url(self.dsn)

        try:
            return await self.protocol.channel()
        except aioamqp.AmqpClosedConnection:
            self.protocol = None  # Drop connection cache
            raise

    def _amqp_wrapper(self, handler):
        async def _wrapper(channel, body, envelope, properties):
            data = handler.queue.deserialize(body)
            data['amqp'] = MessageMeta(
                queue=handler.queue, channel=channel,
                envelope=envelope, properties=properties)

            self.log.debug('Calling %s by %s with %s', handler.__qualname__,
                handler.queue.name, str(data).encode('utf-8'))

            try:
                response = handler(**data)
            except TypeError:
                self.log.error('Call %s failed', handler.__qualname__, exc_info=True)
                return

            if asyncio.iscoroutine(response):
                timer = datetime.now().timestamp()

                try:
                    await asyncio.wait_for(response, handler.timeout)
                except asyncio.TimeoutError:
                    self.log.fatal('TimeoutError: %s %.2f', handler.__qualname__,
                        datetime.now().timestamp() - timer)
                    return

            if handler.options.get('autoack', True):
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        return _wrapper

    async def declare_queue(self, name, **options):
        async with ChannelContext(self, once=True) as channel:
            info = await channel.queue_declare(name, **options)
            self.log.info('Declare/get queue "%(queue)s" with %(message_count)s '
                'messages, %(consumer_count)s consumers', info)

    async def queue_length(self, name):
        async with ChannelContext(self) as channel:
            info = await channel.queue_declare(name)
            return int(info['message_count'])
