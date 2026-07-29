# mypy: ignore-errors
from microagent import MicroAgent, Signal, cron, on, periodic, receiver

# single-run
# import os
# from microagent import configure
# cur_dir = os.path.dirname(os.path.realpath(__file__))
# configure('file://' + os.path.join(cur_dir, 'signals.json'))


class UserAgent(MicroAgent):
    @on('pre_start')
    def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @cron('* * * * *', timeout=5)
    async def example_cron_send_message(self):
        self.log.info('Run cron task')
        await self.broker.mailer.send({'text': 'Report text', 'email': 'admin@lwr.pw'})

    @periodic(period=15, timeout=10, start_after=3)
    async def example_periodic_task_send_signal(self):
        self.log.info('Run periodic task')
        await self.bus.user_comment.send('user_agent', user_id=1, text='informer text')

    @receiver(Signal.user_created)
    async def example_signal_receiver_send_message(self, signal, sender, **kwargs):
        self.log.info('Catch signal %s from %s with %s', signal, sender, kwargs)
        await self.broker.mailer.send({'text': 'Welcome text', 'email': 'user@lwr.pw'})

    @receiver(Signal.user_comment)
    async def example_rpc_call(self, **kwargs):
        self.log.info('Catch signal %s', kwargs)
        value = await self.bus.rpc_comments_count.call('user_agent', user_id=1)
        self.log.info('Get value = %s', value)
