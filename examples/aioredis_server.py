import asyncio
import logging
import sys
import os

from microagent import MicroAgent, periodic, receiver, loadcfg, consumer
from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker
from microagent.tools.amqp import AMQPBroker


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = loadcfg('file://' + os.path.join(cur_dir, 'signals.json'))


class UserAgent(MicroAgent):
    def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @periodic(period=5)
    async def periodic_hand(self):
        self.log.info('QQQQQQQQQQQQQQQQQ')

    @periodic(period=15, timeout=10, start_after=3)
    async def periodic_handler(self):
        self.log.info('Run periodic task')
        await self.bus.user_comment.send('user_agent', user_id=1)
        await self.broker.mailer.send({'data': 'asdasd', 'id': 1, 'f': 123})
        print(await self.broker.emailer.length())

    @receiver(signals.user_created)
    async def created_handler(self, **kwargs):
        self.log.info('Catch signal %s', kwargs)

    @receiver(signals.user_comment)
    async def comment_handler(self, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        value = await self.bus.rpc_comments_count.call('user_agent', user_id=1)
        self.log.info('Get value = %s', value)

    @consumer(queues.emailer)
    async def mailer_handler(self, **kwargs):
        self.log.info('E-Mailer %s %s', self, kwargs)

async def _main():
    bus = AIORedisSignalBus('redis://localhost/7')
    #broker = AMQPBroker('amqp://user:31415@localhost:5672/prod')
    broker = AIORedisBroker('redis://localhost/7')
    print('Broker', broker)
    await broker.mailer.declare()
    await broker.emailer.declare()
    await bus.started.send('user_agent')
    UserAgent(bus, broker=broker)


def main():
    asyncio.ensure_future(_main())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(main)
    loop.run_forever()
