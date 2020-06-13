import signal
import asyncio
import functools
from microagent import MicroAgent, on

class MyAgent(MicroAgent):
    @on('pre_start')
    async def call_first(self):
        print('Call before recivers and consumers will be enabled')

    @on('post_start')
    async def call_second(self):
        print('Call after recivers and consumers will be enabled')

    @on('pre_stop')
    async def call_last(self):
        print('Call when agent.stop() called')


async def run():
    agent = MyAgent()
    await agent.start()
    print('Working...')
    await agent.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
