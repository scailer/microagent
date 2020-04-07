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
from dataclasses import dataclass
from typing import Union, Optional, Callable


class PeriodicMixin:
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
        self._start()


@dataclass(frozen=True)
class PeriodicTask(PeriodicMixin):
    agent: 'microagent.MicroAgent'
    handler: Callable
    period: Union[int, float]
    timeout: Union[int, float]
    start_after: Optional[Union[int, float]]

    def __repr__(self) -> str:
        return f'<PeriodicTask {self.handler.__name__} of {self.agent} every {self.period} sec>'

    def _start(self) -> None:
        asyncio.get_running_loop().call_later(self.start_after, _periodic, self)


def _periodic(task: PeriodicTask) -> asyncio.Task:
    asyncio.get_running_loop().call_later(task.period, _periodic, task)
    return asyncio.create_task(task.call())


@dataclass(frozen=True)
class CRONTask(PeriodicMixin):
    agent: 'microagent.MicroAgent'
    handler: Callable
    croniter: Union[int, float]
    timeout: Union[int, float]

    def __repr__(self) -> str:
        return f'<CRONTask {self.handler.__name__} of {self.agent} every {self.croniter}>'

    def _start(self) -> None:
        start_after = self.croniter.get_next(float) - time.time()
        asyncio.get_running_loop().call_later(start_after, self.handler)


def _cron(task: CRONTask) -> asyncio.Task:
    delay = task.croniter.get_next(float) - time.time()
    asyncio.get_running_loop().call_later(delay, _cron, task)
    task.agent.log.debug('Run %s', task)
    return asyncio.create_task(task.call())
