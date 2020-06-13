'''
The MicroAgent method can be run periodically after a certain period of time or
on a schedule (cron).

Periodic calls are implemented with *asyncio.call_later* chains.
Before each method call, the next call is initiated.
Each call is independent, and previous calls do not affect subsequent calls.
Exceptions are written to the logger in the associated Microagent.

.. code-block:: python

    class Agent(MicroAgent):

        @periodic(period=3, timeout=10, start_after=2)  # in seconds
        async def periodic_handler(self):
            pass  # code here

        @cron('*/10 * * * *', timeout=10)  # in seconds
        async def cron_handler(self):
            pass  # code here
'''
import time
import asyncio
import inspect
import croniter
from dataclasses import dataclass
from typing import Union, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .agent import MicroAgent


class PeriodicMixin:
    agent: 'MicroAgent'
    handler: Callable
    timeout: float

    async def call(self) -> None:
        response = None

        try:
            response = self.handler()
        except Exception as exc:
            self.agent.log.fatal(f'Periodic Exception: {exc}', exc_info=True)

        if inspect.isawaitable(response):
            try:
                await asyncio.wait_for(response, self.timeout)
            except asyncio.TimeoutError:
                self.agent.log.fatal(f'TimeoutError: {self}')
            except Exception as exc:
                self.agent.log.fatal(f'Periodic Exception: {exc}', exc_info=True)

    def start(self, start_after: float) -> None:
        asyncio.get_running_loop().call_later(start_after, _periodic, self)


@dataclass(frozen=True)
class PeriodicTask(PeriodicMixin):
    agent: 'MicroAgent'
    handler: Callable
    period: float
    timeout: float
    start_after: float

    def __repr__(self) -> str:
        return f'<PeriodicTask {self.handler.__name__} of {self.agent} every {self.period} sec>'


@dataclass(frozen=True)
class CRONTask(PeriodicMixin):
    agent: 'MicroAgent'
    handler: Callable
    cron: croniter.croniter
    timeout: float

    def __repr__(self) -> str:
        return f'<CRONTask {self.handler.__name__} of {self.agent} every {self.cron}>'

    @property
    def start_after(self) -> float:
        '''
            *start_after* property of **CRONTask** object is a next value of
            generator behind facade. Be carefully with manual manipulation with it.
        '''
        return self.cron.get_next(float) - time.time()  # increment counter, return initial delay

    @property
    def period(self) -> float:
        '''
            *period* property of **CRONTask** object is a next value of
            generator behind facade. Be carefully with manual manipulation with it.
        '''
        self.agent.log.debug('Run %s', self)
        return self.cron.get_next(float) - time.time()  # increment counter, return next step delay


def _periodic(task: Union[PeriodicTask, CRONTask]) -> asyncio.Task:
    asyncio.get_running_loop().call_later(task.period, _periodic, task)
    return asyncio.create_task(task.call())
