import os
import sys
import time
import asyncio
import logging

from microagent import MicroAgent, periodic, receiver, load_stuff
from microagent.tools.aioredis import AIORedisSignalBus

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'events.json'))


class SenderAgent(MicroAgent):
    @periodic(period=5, timeout=5)
    async def sender(self):
        self.log.info('Begin at %s', time.asctime())
        response = await self.bus.my_event.call('sender_name', val1=1, val2='a')
        self.log.info('Finish at %s with %s', time.asctime(), response)


class CatcherAgent(MicroAgent):
    @receiver(signals.my_event)
    async def catcher(self, signal, sender, val1, val2):
        self.log.info(f'Catch {signal} {sender} {val1} {val2}')
        await asyncio.sleep(3)
        return 1


async def main():
    bus = AIORedisSignalBus('redis://localhost/7')
    await SenderAgent(bus=bus).start()
    await CatcherAgent(bus=bus).start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(lambda: asyncio.ensure_future(main()))
    loop.run_forever()
