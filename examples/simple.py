import asyncio
from microagent import MicroAgent, periodic

class MyAgent(MicroAgent):
    @periodic(period=1, timeout=1)
    async def hello(self):
        print('Hellow world!')

async def main():
    await MyAgent().start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(lambda: asyncio.ensure_future(main()))
    loop.run_forever()
