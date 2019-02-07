import asyncio
import logging
import sys
import os
from collections import defaultdict

from microagent import MicroAgent, receiver, consumer, load_stuff
from microagent.tools.pulsar import MicroAgentApp


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class CommentAgent(MicroAgent):
    def setup(self):
        self.log.info('Run ...\n %s', self.info())
        self.comments_cache = defaultdict(lambda: 0)

    @receiver(signals.user_comment)
    async def comment_handler(self, user_id, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        self.comments_cache[user_id] += 1
        await self.broker.emailer.send({'data': 'asdasd', 'asd': 123})
        await self.broker.emailer.length()

    @receiver(signals.rpc_comments_count)
    async def count_handler(self, user_id, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        await asyncio.sleep(1)
        return self.comments_cache[user_id]

    @consumer(queues.mailer)
    async def mail_handler(self, **kwargs):
        self.log.info('Mailer %s %s', self, kwargs)


def main():
    MicroAgentApp(agent=CommentAgent).start()


if __name__ == '__main__':  # pragma nocover
    main()
