# mypy: ignore-errors
import os
import asyncio
from collections import defaultdict
from microagent import MicroAgent, on, receiver, load_stuff

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class CommentAgent(MicroAgent):
    @on('pre_start')
    async def setup(self):
        self.log.info('Run ...\n %s', self.info())
        self.comments_cache = defaultdict(lambda: 0)
        self.counter = 0

    @receiver(signals.rpc_comments_count)
    async def example_rpc_handler(self, user_id, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        await asyncio.sleep(1)
        return self.comments_cache[user_id]

    @receiver(signals.user_comment)
    async def example_signal_receiver_send_message(self, user_id, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        self.comments_cache[user_id] += 1
        await self.broker.mailer.send({'text': 'Comment', 'email': 'user@lwr.pw'})
        await self.broker.mailer.length()
