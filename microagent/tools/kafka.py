'''
:ref:`Queue Broker <broker>` based on :kafka:`kafka <>`.
'''
import urllib
import asyncio
import logging

from datetime import datetime

import aiokafka  # type: ignore

from ..broker import AbstractQueueBroker, Consumer


class KafkaBroker(AbstractQueueBroker):
    '''
        Experimental broker based on the Apache Kafka distributed stream processing system.

        :param dsn: string, data source name for connection kafka://localhost:9092
        :param logger: logging.Logger (optional)


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
    addr: str
    producer: aiokafka.AIOKafkaProducer

    def __init__(self, dsn: str, logger: logging.Logger = None) -> None:
        super().__init__(dsn, logger)
        _loop = asyncio.get_running_loop()
        self.addr = urllib.parse.urlparse(dsn).netloc
        self.producer = aiokafka.AIOKafkaProducer(loop=_loop, bootstrap_servers=self.addr)

    async def send(self, name: str, message: str, **kwargs) -> None:
        await self.producer.start()

        try:
            await self.producer.send_and_wait(name, bytes(message, 'utf8'), **kwargs)

        finally:
            await self.producer.stop()
            _loop = asyncio.get_running_loop()
            self.producer = aiokafka.AIOKafkaProducer(loop=_loop, bootstrap_servers=self.addr)

    async def bind(self, name: str):
        loop = asyncio.get_running_loop()
        kafka_consumer = aiokafka.AIOKafkaConsumer(name, loop=loop, bootstrap_servers=self.addr)
        asyncio.create_task(self._kafka_wrapper(kafka_consumer, name))

    async def _kafka_wrapper(self, kafka_consumer: aiokafka.AIOKafkaConsumer, name: str) -> None:
        consumer = self._bindings[name]
        await kafka_consumer.start()

        try:
            async for msg in kafka_consumer:
                data = consumer.queue.deserialize(msg.value)  # type: dict
                data['kafka'] = msg
                asyncio.create_task(self._handle(consumer, data))

        finally:
            await kafka_consumer.stop()

    async def _handle(self, consumer: Consumer, data: dict) -> None:
        self.log.debug('Calling %s by %s with %s', consumer, consumer.queue.name, data)

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
                self.log.error('TimeoutError: %s %.2f', consumer,
                    datetime.now().timestamp() - timer)

    async def queue_length(self, name: str, **options):
        pass  # TODO: get queue length
