import os
import asyncio
from microagent import MicroAgent, on, periodic, receiver, consumer, cron, load_stuff
from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class EmailAgent(MicroAgent):
    @on('pre_start')
    async def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @consumer(queues.mailer)
    async def example_read_queue(self, **kwargs):
        self.log.info('Catch emailer %s', kwargs)
