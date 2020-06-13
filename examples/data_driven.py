import os
import asyncio

from microagent import MicroAgent, periodic, consumer, load_stuff
from microagent.tools.aioredis import AIORedisBroker

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'queues.json'))


class SenderAgent(MicroAgent):
    @periodic(period=1, timeout=1)
    async def sender(self):
        await self.broker.my_queue.send({'val1': 1, 'val2': 'a'})


class CatcherAgent(MicroAgent):
    @consumer(queues.my_queue)
    async def catcher(self, val1, **data):
        print(f'Catch {val1} {data}')


async def main():
    broker = AIORedisBroker('redis://localhost/7')
    await SenderAgent(broker=broker).start()
    await CatcherAgent(broker=broker).start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(lambda: asyncio.ensure_future(main()))
    loop.run_forever()
