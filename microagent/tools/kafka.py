'''
:ref:`Queue Broker <broker>` based on :kafka:`kafka <>`.
'''
import asyncio
import time

from dataclasses import dataclass, field
from typing import Any
from urllib import parse

import aiokafka

from ..broker import AbstractQueueBroker, Consumer


@dataclass
class KafkaBroker(AbstractQueueBroker):
    '''
        Experimental broker based on the Apache Kafka distributed stream processing system.

        :param dsn: string, data source name for connection kafka://localhost:9092
        :param log: logging.Logger (optional)


        Sending messages.

        .. code-block:: python

            from microagent.tools.kafka import KafkaBroker

            broker = KafkaBroker('kafka://localhost:9092')

            await broker.user_created.send({'user_id': 1})


        Consuming messages.

        .. code-block:: python

            class EmailAgent(MicroAgent):
                @consumer(queues.mailer)
                async def example_read_queue(self, kafka, **data):
                    # kafka: AIOKafkaConsumer
                    process(data)
    '''
    addr: str = field(init=False)
    producer: aiokafka.AIOKafkaProducer = field(init=False)

    def __post_init__(self) -> None:
        self.addr = parse.urlparse(self.dsn).netloc
        self.producer = aiokafka.AIOKafkaProducer(bootstrap_servers=self.addr)

    async def send(self, name: str, message: str, **kwargs: Any) -> None:
        await self.producer.start()

        try:
            await self.producer.send_and_wait(name, bytes(message, 'utf8'), **kwargs)

        finally:
            await self.producer.stop()
            _loop = asyncio.get_running_loop()
            self.producer = aiokafka.AIOKafkaProducer(loop=_loop, bootstrap_servers=self.addr)

    async def bind(self, name: str) -> None:
        loop = asyncio.get_running_loop()
        kafka_consumer = aiokafka.AIOKafkaConsumer(name, loop=loop, bootstrap_servers=self.addr)
        asyncio.create_task(self._kafka_wrapper(kafka_consumer, name))

    async def _kafka_wrapper(self, kafka_consumer: aiokafka.AIOKafkaConsumer, name: str) -> None:
        consumer = self._bindings[name]
        await kafka_consumer.start()

        try:
            async for msg in kafka_consumer:
                data = self.prepared_data(consumer, msg.value)
                data['kafka'] = msg
                asyncio.create_task(self._handle(consumer, data))

        finally:
            await kafka_consumer.stop()

    async def _handle(self, consumer: Consumer, data: dict) -> None:
        self.log.debug('Calling %s by %s with %s', consumer, consumer.queue.name, data)
        timer = time.monotonic()

        try:
            await asyncio.wait_for(consumer.handler(**data), consumer.timeout)
        except TypeError:
            self.log.exception('Call %s failed', consumer)
        except asyncio.TimeoutError:
            self.log.error('TimeoutError: %s %.2f', consumer, time.monotonic() - timer)

    async def queue_length(self, name: str, **options: Any) -> int:  # noqa
        return 0
