import asyncio
import logging
import sys
import os

import aioredis

from microagent import MicroAgent, periodic, receiver, load_signals
from microagent.tools.aioredis import AIORedisSignalBus


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals = load_signals('file://' + os.path.join(cur_dir, 'signals.json'))


class UserAgent(MicroAgent):
    def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @periodic(period=15, timeout=10, start_after=3)
    async def periodic_handler(self):
        self.log.info('Run periodic task')
        await self.bus.user_comment.send('user_agent', user_id=1)

    @receiver(signals.user_created)
    async def created_handler(self, **kwargs):
        self.log.info('Catch signal %s', kwargs)

    @receiver(signals.user_comment)
    async def comment_handler(self, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        value = await self.bus.rpc_comments_count.call('user_agent', user_id=1)
        self.log.info('Get value = %s', value)


async def _main():
    bus = AIORedisSignalBus('redis://localhost')
    await bus.started.send('user_agent')
    UserAgent(bus)


def main():
    asyncio.ensure_future(_main())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(main)
    loop.run_forever()
