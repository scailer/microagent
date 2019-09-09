import asyncio
import os

from microagent import load_stuff
from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker
from microagent.tools.amqp import AMQPBroker
from microagent.tools.kafka import KafkaBroker

from agents import MentionAgent


# Загружаем доступные сигналы и очереди из файла
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


async def main():
    aioredis_bus = AIORedisSignalBus('redis://localhost/7', prefix='MYAPP')

    # Инициализируем брокеры с разными бекендами
    aioredis_broker = AIORedisBroker('redis://localhost/7')
    amqp_broker = AMQPBroker('amqp://user:31415@localhost:5672/prod')
    kafka_broker = KafkaBroker('kafka://localhost:9092')

    # Используем шину для отправки сообщений независимо от микроагента
    await aioredis_broker.started.send({'service_name': 'my_app'})

    agent = MentionAgent(bus=aioredis_bus, broker=aioredis_broker)
    await agent.start()  # запускаем


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.ensure_future(main))
    loop.run_forever()
