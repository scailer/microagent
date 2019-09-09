import asyncio
from microagent import MicroAgent, periodic, cron

class MyAgent(MicroAgent):
    @periodic(period=1, timeout=1, start_after=5)
    async def hello(self):
        print('Hellow world!')

    @cron('*/2 * * * *', timeout=10)
    async def two_min(self):
        print('Run every 2 min')

async def main():
    await MyAgent().start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(lambda: asyncio.ensure_future(main()))
    loop.run_forever()
