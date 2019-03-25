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
import functools
from typing import Union, Optional
from croniter import croniter


async def _wrap(self, func):
    response = None

    try:
        response = func(self)
    except Exception as e:
        self.log.fatal(f'Periodic Exception: {e}', exc_info=True)

    if inspect.isawaitable(response):
        try:
            await asyncio.wait_for(response, func._timeout)
        except asyncio.TimeoutError:
            self.log.fatal(f'TimeoutError: {func.__qualname__}')
        except Exception as e:
            self.log.fatal(f'Periodic Exception: {e}', exc_info=True)


def _periodic(self, func):
    self._loop.call_later(
        func._period,
        lambda *args: asyncio.ensure_future(_periodic(*args)),
        self, func)
    return _wrap(self, func)


def periodic(period: Union[int, float], timeout: Optional[Union[int, float]] = 1,
        start_after: Union[int, float] = None):
    '''
        Decorator witch mark :class:`MicroAgent` method as periodic function

        :param period: Period of running functions in seconds
        :param timeout: Function timeout in seconds
        :param start_after: Delay for running loop in seconds
    '''

    assert period > 0.001, 'period must be more than 0.001 s'
    assert timeout > 0.001, 'timeout must be more than 0.001 s'
    if start_after:
        assert start_after >= 0, 'start_after must be a positive'

    def _decorator(func):
        func.__periodic__ = True
        func._period = period
        func._timeout = timeout

        @functools.wraps(func)
        def _call(self):
            asyncio.ensure_future(_periodic(self, self.hook.decorate(func)), loop=self._loop)

        _call.origin = func
        _call._start_after = start_after
        return _call

    return _decorator


def _cron(self, func):
    self._loop.call_later(
        func._croniter.get_next(float) - time.time(),
        lambda *args: asyncio.ensure_future(_cron(*args)),
        self, func)
    self.log.debug(f'Run cron task %s', func)
    return _wrap(self, func)


def cron(spec: str, timeout: Optional[Union[int, float]] = 1):
    '''
        Decorator witch mark :class:`MicroAgent` method as shceduling (cron) function

        :param spec: Specified running shceduling in cron format
        :param timeout: Function timeout in seconds
    '''

    assert timeout > 0.001, 'timeout must be more than 0.001 s'

    def _decorator(func):
        func.__periodic__ = True
        func._croniter = croniter(spec, time.time())
        func._timeout = timeout

        @functools.wraps(func)
        def _call(self):
            asyncio.ensure_future(_cron(self, self.hook.decorate(func)), loop=self._loop)

        _call.origin = func
        _call._start_after = func._croniter.get_next(float) - time.time()
        return _call

    return _decorator
