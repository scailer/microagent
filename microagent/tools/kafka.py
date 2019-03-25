import urllib
import asyncio
import logging

from typing import Optional
from datetime import datetime

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from ..broker import AbstractQueueBroker


class KafkaBroker(AbstractQueueBroker):

    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        super().__init__(dsn, logger)
        self.addr = urllib.parse.urlparse(dsn).netloc
        self.producer = AIOKafkaProducer(loop=self._loop, bootstrap_servers=self.addr)

    async def send(self, name: str, message: str, **kwargs):
        if not self.producer._closed:
            await self.producer.start()

        try:
            await self.producer.send_and_wait(name, bytes(message, 'utf8'), **kwargs)

        finally:
            await self.producer.stop()
            self.producer = AIOKafkaProducer(loop=self._loop, bootstrap_servers=self.addr)

    async def bind(self, name: str, handler):
        consumer = AIOKafkaConsumer(name, loop=self._loop, bootstrap_servers=self.addr)
        asyncio.ensure_future(self._kafka_wrapper(consumer, handler))

    async def _kafka_wrapper(self, consumer, handler):
        await consumer.start()

        try:
            async for msg in consumer:
                data = handler.queue.deserialize(msg.value)
                data['kafka'] = msg
                asyncio.ensure_future(self._handle(handler, data))

        finally:
            await consumer.stop()

    async def _handle(self, handler, data):
        self.log.debug('Calling %s by %s with %s', handler.__qualname__,
            handler.queue.name, data)

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

    async def queue_length(self, name, **options):
        pass  # TODO: get queue length
