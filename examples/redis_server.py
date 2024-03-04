# mypy: ignore-errors
import sys
import asyncio
import logging
from microagent.tools.redis import RedisSignalBus, RedisBroker

from user_agent import UserAgent
from comment_agent import CommentAgent
from email_agent import EmailAgent

logging.basicConfig(format=(
    '%(levelname)-8s [pid#%(process)d] %(asctime)s %(name)s '
    '%(filename)s:%(lineno)d %(message)s'
), stream=sys.stdout, level=logging.DEBUG)


async def main():
    bus = RedisSignalBus('redis://localhost/7')
    broker = RedisBroker('redis://localhost/7')
    await bus.started.send('user_agent')

    user_agent = UserAgent(bus=bus, broker=broker)
    comment_agent = CommentAgent(bus=bus, broker=broker)
    email_agent = EmailAgent(broker=broker)

    await user_agent.start()
    await comment_agent.start()
    await email_agent.start()

    while True:
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(main())
