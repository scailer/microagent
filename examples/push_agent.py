# mypy: ignore-errors
import asyncio
import logging
import os
import sys

from microagent import MicroAgent, consumer, load_stuff, on, periodic
from microagent.tools import amqp, redis

logging.basicConfig(format=(
    '%(levelname)-8s [pid#%(process)d] %(asctime)s %(name)s '
    '%(filename)s:%(lineno)d %(message)s'
), stream=sys.stdout, level=logging.INFO)

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class UserAgent(MicroAgent):
    @on('pre_start')
    def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @periodic(period=15, timeout=10, start_after=3)
    async def example_periodic_task_send_push(self):
        self.log.info('Run periodic task')
        await self.broker.push3.send({'text': 'informer text'}, topic='msg.ios')
        await self.broker.push.send({'text': 'informer 1'})

    @consumer(queues.android)
    async def example_read_queue_android(self, **kwargs):
        self.log.info('Catch android %s', kwargs)

    @consumer(queues.ios)
    async def example_read_queue_ios(self, **kwargs):
        self.log.info('Catch ios %s', kwargs)

    @consumer(queues.android_a)
    async def example_read_queue_android_a(self, **kwargs):
        self.log.info('Catch android_a %s', kwargs)

    @consumer(queues.ios_a)
    async def example_read_queue_ios_a(self, **kwargs):
        self.log.info('Catch ios_a %s', kwargs)


async def main():
    bus = redis.RedisSignalBus('redis://localhost/7')
    broker = amqp.AMQPBroker('amqp://guest:guest@localhost:5672/')
    await broker.push3.send({'q':1}, topic='msg.android')

    agent = UserAgent(bus=bus, broker=broker)
    await agent.start()

    while True:
        await asyncio.sleep(1)


asyncio.run(main())
