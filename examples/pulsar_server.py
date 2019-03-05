import asyncio
import logging
import time
import sys
import os
from collections import defaultdict

from microagent import MicroAgent, receiver, load_stuff, on, periodic
from microagent.tools.pulsar import MicroAgentApp


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class CommentAgent(MicroAgent):
    @on('pre_start')
    def setup(self):
        self.log.info('Run ...\n %s', self.info())
        self.comments_cache = defaultdict(lambda: 0)
        self.counter = 0

    @on('post_start')
    def post_start_handler(self):
        self.log.info('POST')

    @on('pre_spam2')
    async def on_pre_spam(self, context):
        context['timer'] = time.time()
        self.log.info('PRE SPAM2 %s', context)

    @periodic(period=1, timeout=1)
    async def spam2(self):
        self.log.info('SPAM2')

    @on('post_spam2')
    async def on_post_spam(self, response, context):
        self.log.info('POST SPAM2 %s', context)
        self.log.info('SPAM TIMER %s', time.time() - context['timer'])

    @on('error_spam')
    async def on_error_spam(self, exc, context):
        self.log.info('ERR SPAM %s %s', exc, context)

    @periodic(period=1, timeout=1)
    async def spam(self):
        self.log.info('SPAM')
        reqs = []
        for x in range(1):
            self.counter += 1
            await self.broker.emailer.send({'data': 'spam', 'counter': self.counter})
            self.log.info('QTY %s', self.counter)
        asyncio.ensure_future(asyncio.gather(*reqs, loop=self._loop))

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

#    @consumer(queues.mailer)
#    async def mail_handler(self, **kwargs):
#        self.log.info('Mailer %s %s', self, kwargs)


def main():
    MicroAgentApp(agent=CommentAgent).start()


if __name__ == '__main__':  # pragma nocover
    main()
