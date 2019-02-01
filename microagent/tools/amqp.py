import logging
import asyncio

from collections import namedtuple
from datetime import datetime
from typing import Optional

import aioamqp
from ..broker import AbstractQueueBroker


MessageMeta = namedtuple('MessageMeta', ['queue', 'channel', 'envelope', 'properties'])


class AMQPBroker(AbstractQueueBroker):
    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        super().__init__(dsn, logger)
        self.protocol = None

    async def send(self, name: str, message: str, **kwargs):
        kwargs['exchange_name'] = kwargs.get('exchange_name', '')
        channel = await self.get_channel()
        await channel.basic_publish(message, routing_key=name, **kwargs)

    async def bind(self, name, handler):
        try:
            _, protocol = await aioamqp.from_url(self.dsn)
        except aioamqp.AmqpClosedConnection:
            return self.log.fatal('AmqpClosedConnection')

        channel = await protocol.channel()
        self.log.debug('Bind %s to queue "%s"', handler, name)
        await channel.basic_consume(self._amqp_wrapper(handler), queue_name=name)

    async def get_channel(self):
        if not self.protocol:
            try:
                _, self.protocol = await aioamqp.from_url(self.dsn)
            except aioamqp.AmqpClosedConnection:
                return self.log.fatal('AmqpClosedConnection')
        return await self.protocol.channel()

    def _amqp_wrapper(self, handler):
        async def _wrapper(channel, body, envelope, properties):
            data = handler.queue.deserialize(body)
            data['amqp'] = MessageMeta(
                queue=handler.queue, channel=channel,
                envelope=envelope, properties=properties)

            self.log.debug('Calling %s by %s with %s', handler.__qualname__,
                handler.queue.name, data)

            try:
                response = handler(**data)
            except TypeError:
                self.log.error('Call %s failed', handler.queue.name, exc_info=True)
                return

            if asyncio.iscoroutine(response):
                timer = datetime.now().timestamp()

                try:
                    response = await asyncio.wait_for(response, handler.timeout)
                except asyncio.TimeoutError:
                    self.log.fatal('TimeoutError: %s %.2f', handler.__qualname__,
                        datetime.now().timestamp() - timer)
                    return

            if handler.options.get('autoack', True):
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        return _wrapper

    async def declare_queue(self, name, **options):
        channel = await self.get_channel()
        info = await channel.queue_declare(name, **options)
        self.log.info('Declare/get queue "%(queue)s" with %(message_count)s '
                      'messages, %(consumer_count)s consumers', info)

    async def queue_length(self, name):
        channel = await self.get_channel()
        info = await channel.queue_declare(name)
        return int(info['message_count'])
