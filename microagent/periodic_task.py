''' :class:`MicroAgent` method can be runing periodicaly by time period or shedule (cron)

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
from typing import Union, Optional, Callable


class PeriodicMixin:
    agent: 'microagent.MicroAgent'
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

    def start(self) -> None:
        asyncio.get_running_loop().call_later(self.start_after, _periodic, self)


@dataclass(frozen=True)
class PeriodicTask(PeriodicMixin):
    agent: 'microagent.MicroAgent'
    handler: Callable
    period: float
    timeout: float
    start_after: float

    def __repr__(self) -> str:
        return f'<PeriodicTask {self.handler.__name__} of {self.agent} every {self.period} sec>'


@dataclass(frozen=True)
class CRONTask(PeriodicMixin):
    agent: 'microagent.MicroAgent'
    handler: Callable
    cron: croniter.croniter
    timeout: float

    def __repr__(self) -> str:
        return f'<CRONTask {self.handler.__name__} of {self.agent} every {self.cron}>'

    @property
    def start_after(self) -> float:
        return self.cron.get_next(float) - time.time()

    @property
    def period(self) -> float:
        self.agent.log.debug('Run %s', self)
        return self.cron.get_next(float) - time.time()


def _periodic(task: Union[PeriodicTask, CRONTask]) -> asyncio.Task:
    asyncio.get_running_loop().call_later(task.period, _periodic, task)
    return asyncio.create_task(task.call())
