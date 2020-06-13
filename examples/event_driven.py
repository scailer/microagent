import os
import asyncio

from microagent import MicroAgent, periodic, receiver, load_stuff
from microagent.tools.aioredis import AIORedisSignalBus

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'events.json'))


class SenderAgent(MicroAgent):
    @periodic(period=1, timeout=1)
    async def sender(self):
        await self.bus.my_event.send('sender_name', val1=1, val2='a')


class CatcherAgent(MicroAgent):
    @receiver(signals.my_event)
    async def catcher(self, signal, sender, val1, val2):
        print(f'Catch {signal} {sender} {val1} {val2}')


async def main():
    bus = AIORedisSignalBus('redis://localhost/7')
    await SenderAgent(bus=bus).start()
    await CatcherAgent(bus=bus).start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(lambda: asyncio.ensure_future(main()))
    loop.run_forever()
