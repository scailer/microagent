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
import asyncio
import inspect
import re
import time

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, ClassVar, NamedTuple, TypedDict

from .abc import BoundKey, PeriodicFunc


if TYPE_CHECKING:
    from .agent import MicroAgent


RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
MAX_DIFF = 2 * 356 * 24 * 60 * 60


class CRON(NamedTuple):
    spec: str
    minutes: list[int]  # 0-59
    hours: list[int]  # 0-23
    days: list[int]  # 1-31
    months: list[int]  # 1-12
    weekdays: list[int]  # 0-7

    def next(self) -> datetime:  # noqa A003
        return next_moment(self, datetime.now(tz=timezone.utc))

    def __str__(self) -> str:
        return f'[{self.spec}]'


class PeriodicMixin:
    agent: 'MicroAgent'
    handler: Callable
    timeout: float

    async def call(self) -> None:
        try:
            response = self.handler()

            if inspect.isawaitable(response):
                try:
                    await asyncio.wait_for(response, self.timeout)
                except asyncio.TimeoutError:
                    self.agent.log.exception(f'TimeoutError: {self}')

        except Exception as exc:
            self.agent.log.exception(f'Periodic Exception: {exc}')

    def start(self, start_after: float) -> None:
        asyncio.get_running_loop().call_later(start_after, _periodic, self)  # type: ignore[arg-type]


class PeriodicArgs(TypedDict):
    period: float
    timeout: float
    start_after: float


@dataclass(frozen=True)
class PeriodicTask(PeriodicMixin):
    agent: 'MicroAgent'
    handler: PeriodicFunc
    period: float
    timeout: float
    start_after: float

    _register: ClassVar[dict[BoundKey, PeriodicArgs]] = {}

    def __repr__(self) -> str:
        return f'<PeriodicTask {self.handler.__name__} of {self.agent} every {self.period} sec>'


class CRONArgs(TypedDict):
    cron: CRON
    timeout: float


@dataclass(frozen=True)
class CRONTask(PeriodicMixin):
    agent: 'MicroAgent'
    handler: PeriodicFunc
    cron: CRON
    timeout: float

    _register: ClassVar[dict[BoundKey, CRONArgs]] = {}

    def __repr__(self) -> str:
        return f'<CRONTask {self.handler.__name__} of {self.agent} every {self.cron}>'

    @property
    def start_after(self) -> float:
        '''
            *start_after* property of **CRONTask** object is a next value of
            generator behind facade. Be carefully with manual manipulation with it.
        '''
        return self.cron.next().timestamp() - time.time()  # initial delay

    @property
    def period(self) -> float:
        '''
            *period* property of **CRONTask** object is a next value of
            generator behind facade. Be carefully with manual manipulation with it.
        '''
        self.agent.log.debug('Run %r', self)
        return self.cron.next().timestamp() - time.time()  # next step delay


def _periodic(task: PeriodicTask | CRONTask) -> asyncio.Task:
    asyncio.get_running_loop().call_later(task.period, _periodic, task)
    return asyncio.create_task(task.call())


def cron_parser(spec: str) -> CRON:
    '''
        * * * * *
    '''

    values = []
    norm_spec = (
        re.sub(  # * -> 0-23/1
            r'^\*$',
            r'%d-%d/1' % rng,
            re.sub(  # */2 -> 0-23/2
                r'^\*(\/.+)$',
                r'%d-%d\1' % rng,
                re.sub(  # 5-20 -> 5-20/1
                    r'^(\d+-\d+)$',
                    r'\1/1',
                    val
                )
            )
        )
        for rng, val in zip(RANGES, spec.split(), strict=True)
    )

    for i, val in enumerate(norm_spec):
        match = re.search(r'^([^-]+)-([^-/]+)/(\d+)?$', val)

        if match:  # 0-23/5 -> [0, 5, 10, 15, 20]
            _min, _max, _step = map(int, match.groups())

            if i in {2, 3} and _min == 1:
                values.append([x for x in range(_min - 1, _max + 1) if x and not x % _step])
            else:
                values.append([x for x in range(_min, _max + 1) if not x % _step])

        else:  # 4,7,12 -> [4, 7, 12]
            values.append([int(x) for x in val.split(',')])

    return CRON(
        spec=spec,
        minutes=values[0],
        hours=values[1],
        days=values[2],
        months=values[3],
        weekdays=values[4]
    )


def next_moment(cron: CRON, now: datetime) -> datetime:
    if abs((datetime.now(tz=timezone.utc) - now).total_seconds()) > MAX_DIFF:
        raise ValueError

    if now.second or now.microsecond:
        # if moment passed several seconds ago, go to next minute
        now += timedelta(minutes=1)
        now = datetime(year=now.year, month=now.month, day=now.day,
            hour=now.hour, minute=now.minute, tzinfo=timezone.utc)

    if now.month not in cron.months:
        if now.month == 12:  # noqa PLR2004
            now = datetime(year=now.year + 1, month=1, day=1, tzinfo=timezone.utc)
        else:
            now = datetime(year=now.year, month=now.month + 1, day=1, tzinfo=timezone.utc)

        return next_moment(cron, now)

    if now.day not in cron.days or now.weekday() not in cron.weekdays:
        now += timedelta(days=1)
        now = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
        return next_moment(cron, now)

    if now.hour not in cron.hours:
        now += timedelta(hours=1)
        now = datetime(year=now.year, month=now.month, day=now.day, hour=now.hour,
            tzinfo=timezone.utc)
        return next_moment(cron, now)

    if now.minute not in cron.minutes:
        now += timedelta(minutes=1)
        return next_moment(cron, now)

    return datetime(year=now.year, month=now.month, day=now.day,
        hour=now.hour, minute=now.minute, tzinfo=timezone.utc)
