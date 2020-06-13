import asyncio
import os

from microagent import load_stuff
from microagent.tools.aioredis import AIORedisSignalBus  # aioredis based backend
from microagent.tools.pulsar import RedisSignalBus  # pulsar based backend

from agents import UserAgent


# Загружаем доступные сигналы и очереди из файла
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


async def main():
    # Инициализируем шины с разными бекендами
    aioredis_bus = AIORedisSignalBus('redis://localhost/7', prefix='MYAPP')
    pulsar_bus = RedisSignalBus('redis://localhost/7', prefix='MYAPP')

    # Используем шину для отправки сигналов независимо от микроагента
    await pulsar_bus.started.send('user_agent')

    agent = UserAgent(bus=aioredis_bus)  # Инициализируем микроагент и ...
    await agent.start()  # ... запускаем


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.ensure_future(main))
    loop.run_forever()
