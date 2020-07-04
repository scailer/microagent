import time
import asyncio
import croniter
from unittest.mock import MagicMock, AsyncMock
from microagent.periodic_task import PeriodicTask, CRONTask


class Handler(AsyncMock):
    def __name__(self):
        return 'Handler'


async def test_PeriodicTask_call_ok():
    task = PeriodicTask(agent=None, handler=Handler(), period=1, timeout=1, start_after=0)
    assert 'PeriodicTask' in str(task)

    await task.call()
    task.handler.assert_called()


async def test_PeriodicTask_call_fail():
    task = PeriodicTask(agent=MagicMock(), handler=Handler(side_effect=Exception),
        period=1, timeout=1, start_after=0)
    await task.call()
    task.agent.log.fatal.assert_called()


async def test_PeriodicTask_call_fail_timeout():
    async def _foo():
        await asyncio.sleep(.02)

    task = PeriodicTask(agent=MagicMock(), handler=Handler(side_effect=_foo),
        period=1, timeout=.01, start_after=0)
    await task.call()
    task.agent.log.fatal.assert_called()


async def test_PeriodicTask_call_sync_ok():
    task = PeriodicTask(agent=None, handler=MagicMock(), period=1, timeout=1, start_after=0)
    await task.call()
    task.handler.assert_called()


async def test_PeriodicTask_call_sync_fail():
    task = PeriodicTask(agent=MagicMock(), handler=MagicMock(side_effect=Exception),
        period=1, timeout=1, start_after=0)
    await task.call()
    task.agent.log.fatal.assert_called()


async def test_PeriodicTask_start_ok():
    class TestTask(PeriodicTask):
        call = MagicMock(side_effect=Exception)

    task = TestTask(agent=None, handler=Handler(), period=1, timeout=1, start_after=0)
    task.start(task.start_after)


async def test_CRONTask_call_ok():
    task = CRONTask(agent=MagicMock(), handler=Handler(),
        cron=croniter.croniter('* * * * *', time.time()), timeout=1)
    assert 'CRONTask' in str(task)
    assert 0 < task.start_after < 60

    await task.call()
    task.handler.assert_called()

    task = CRONTask(agent=MagicMock(), handler=Handler(),
        cron=croniter.croniter('* * * * *', time.time()), timeout=1)
    assert int(task.start_after + 60) == int(task.period)
