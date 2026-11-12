# mypy: ignore-errors
import asyncio

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from microagent.timer import DAYS, CRONTask, PeriodicTask, cron_parser, next_moment


YEAR = datetime.now(tz=timezone.utc).year


class Handler(AsyncMock):
    def __name__(self) -> str:  # noqa PLW3201
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
    task.agent.log.exception.assert_called()


async def test_PeriodicTask_call_fail_timeout():
    async def _foo():
        await asyncio.sleep(.02)

    task = PeriodicTask(agent=MagicMock(), handler=Handler(side_effect=_foo),
        period=1, timeout=.01, start_after=0)
    await task.call()
    task.agent.log.exception.assert_called()


async def test_PeriodicTask_call_sync_ok():
    task = PeriodicTask(agent=None, handler=MagicMock(), period=1, timeout=1, start_after=0)
    await task.call()
    task.handler.assert_called()


async def test_PeriodicTask_call_sync_fail():
    task = PeriodicTask(agent=MagicMock(), handler=MagicMock(side_effect=Exception),
        period=1, timeout=1, start_after=0)
    await task.call()
    task.agent.log.exception.assert_called()


async def test_PeriodicTask_start_ok():
    class TestTask(PeriodicTask):
        call = MagicMock(side_effect=Exception)

    task = TestTask(agent=None, handler=Handler(), period=1, timeout=1, start_after=0)
    task.start(task.start_after)


async def test_CRONTask_call_ok():
    task = CRONTask(agent=MagicMock(), handler=Handler(),
        cron=cron_parser('* * * * *'), timeout=1)
    assert 'CRONTask' in str(task)
    assert 0 < task.start_after < 60

    await task.call()
    task.handler.assert_called()

    task = CRONTask(agent=MagicMock(), handler=Handler(),
        cron=cron_parser('* * * * *'), timeout=1)
    assert int(task.start_after) == int(task.period)


async def test_cron_parser_ok():
    assert cron_parser('10,20 * * * *').minutes == [10, 20]
    assert cron_parser('15-20 * * * *').minutes == [15, 16, 17, 18, 19, 20]
    assert cron_parser('15-45/5 * * * *').minutes == [15, 20, 25, 30, 35, 40, 45]
    assert cron_parser('*/15 * * * *').minutes == [0, 15, 30, 45]
    assert cron_parser('* */3 * * *').hours == [0, 3, 6, 9, 12, 15, 18, 21]
    assert cron_parser('* * 6-20/5 * *').days == [10, 15, 20]
    assert cron_parser('* * 6-19/5 * *').days == [10, 15]
    assert cron_parser('* * * */3 *').months == [3, 6, 9, 12]


async def test_next_moment_ok_list():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, second=1, tzinfo=timezone.utc)
    assert next_moment(cron_parser('10,20 * * * *'), now) == datetime(
        year=now.year, month=now.month, day=now.day, hour=now.hour, minute=20, tzinfo=timezone.utc)


async def test_next_moment_ok_range():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, second=1, tzinfo=timezone.utc)
    assert next_moment(cron_parser('5-20 * * * *'), now) == datetime(year=now.year,
        month=now.month, day=now.day, hour=now.hour, minute=now.minute + 1, tzinfo=timezone.utc)


async def test_next_moment_ok_range_with_steps():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, second=1, tzinfo=timezone.utc)
    assert next_moment(cron_parser('5-20/5 * * * *'), now) == datetime(
        year=now.year, month=now.month, day=now.day, hour=now.hour, minute=20, tzinfo=timezone.utc)


async def test_next_moment_ok_second():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, second=1, tzinfo=timezone.utc)
    assert next_moment(cron_parser('* * * * *'), now) == datetime(year=now.year, month=now.month,
        day=now.day, hour=now.hour, minute=now.minute + 1, tzinfo=timezone.utc)


async def test_next_moment_ok_minute():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser('* * * * *'), now) == datetime(year=now.year, month=now.month,
        day=now.day, hour=now.hour, minute=now.minute, tzinfo=timezone.utc)


async def test_next_moment_ok_minute_last():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=59, second=1, tzinfo=timezone.utc)
    assert next_moment(cron_parser('* * * * *'), now) == datetime(year=now.year, month=now.month,
        day=now.day, hour=now.hour + 1, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_hour():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser(f'{now.minute - 1} * * * *'), now) == datetime(year=now.year,
        month=now.month, day=now.day, hour=now.hour + 1, minute=now.minute - 1, tzinfo=timezone.utc)


async def test_next_moment_ok_hour_last():
    now = datetime(year=YEAR, month=5, day=15, hour=23, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser('0 * * * *'), now) == datetime(
        year=now.year, month=now.month, day=now.day + 1, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_day():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser(f'0 {now.hour - 1} * * *'), now) == datetime(year=now.year,
        month=now.month, day=now.day + 1, hour=now.hour - 1, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_day_last():
    now = datetime(year=YEAR, month=5, day=31, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser('0 0 * * *'), now) == datetime(
        year=now.year, month=now.month + 1, day=1, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_week():
    n = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser(f'0 0 * * {n.weekday()}'), n - timedelta(days=1)) == datetime(
        year=n.year, month=n.month, day=n.day, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_month():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser(f'0 0 {now.day - 1} * *'), now) == datetime(
        year=now.year, month=now.month + 1, day=now.day - 1, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_month_last():
    now = datetime(year=YEAR, month=12, day=31, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser('0 0 * * *'), now) == datetime(
        year=now.year + 1, month=1, day=1, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_ok_year():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, tzinfo=timezone.utc)
    assert next_moment(cron_parser(f'0 0 1 {now.month - 1} *'), now) == datetime(
        year=now.year + 1, month=now.month - 1, day=1, hour=0, minute=0, tzinfo=timezone.utc)


async def test_next_moment_fail():
    with pytest.raises(ValueError):
        assert next_moment(cron_parser('0 0 2 31 *'), datetime.now(tz=timezone.utc))


async def test_next_moment_ok_latest_day():
    for m, d in enumerate(DAYS[:11], 1):
        assert next_moment(cron_parser('0 0 1 * *'), datetime(year=YEAR, month=m, day=d,
            tzinfo=timezone.utc)) == datetime(year=YEAR, month=m + 1, day=1, tzinfo=timezone.utc)

    assert next_moment(cron_parser('0 0 1 * *'), datetime(year=YEAR, month=12, day=31,
        tzinfo=timezone.utc)) == datetime(year=YEAR + 1, month=1, day=1, tzinfo=timezone.utc)


@pytest.mark.skipif(True, reason='Long test: 2-3 min')
async def test_next_moment_ok_every_minute():
    now = datetime(year=YEAR, month=5, day=15, hour=16, minute=17, second=18, tzinfo=timezone.utc)

    with pytest.raises(ValueError):
        while True:
            now = next_moment(cron_parser('* * * * *'), now) + timedelta(microseconds=1)
