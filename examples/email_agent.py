# mypy: ignore-errors
import os
from microagent import MicroAgent, on, consumer, load_stuff

cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))


class EmailAgent(MicroAgent):
    @on('pre_start')
    async def setup(self):
        self.log.info('Run ...\n %s', self.info())

    @consumer(queues.mailer)
    async def example_read_queue(self, **kwargs):
        self.log.info('Catch emailer %s', kwargs)
