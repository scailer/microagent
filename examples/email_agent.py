# mypy: ignore-errors
from microagent import MicroAgent, Queue, consumer, on

# single-run
# import os
# from microagent import configure
# cur_dir = os.path.dirname(os.path.realpath(__file__))
# configure('file://' + os.path.join(cur_dir, 'signals.json'))


class EmailAgent(MicroAgent):
    @on('pre_start')
    async def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @consumer(Queue.mailer)
    async def example_read_queue(self, **kwargs):
        self.log.info('Catch emailer %s', kwargs)
