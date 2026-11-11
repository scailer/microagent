import asyncio

from collections.abc import Callable


class _Closed:
    ''' Sentinel for closing IterQueue '''

    def __repr__(self) -> str:
        return '<closed>'


_CLOSE = _Closed()


class IterQueue(asyncio.Queue):
    ''' Queue as async generator '''

    def __init__(self) -> None:
        super().__init__()
        self._closed = False

    def close(self) -> None:
        ''' Close queue and interrupt waiting getter with StopAsyncIteration '''
        self._closed = True
        self.put_nowait(_CLOSE)

    def __aiter__(self) -> 'IterQueue':
        return self

    async def __anext__(self) -> dict[str, int | str | None]:
        if self._closed:
            raise StopAsyncIteration

        # External cancellation (graceful shutdown, wait_for) must propagate,
        # it is converted to StopAsyncIteration only by closing the queue
        value = await self.get()

        if value is _CLOSE:
            self.task_done()
            raise StopAsyncIteration

        self.task_done()
        return value


def make_bound_key(func: Callable) -> tuple[str, ...]:
    return func.__module__, *func.__qualname__.split('.')


def raise_timeout(timeout: float, queue: IterQueue) -> asyncio.TimerHandle:
    ''' Interrupt waiting for responses by timer (close the queue) '''

    def _timeout(q: IterQueue) -> None:
        q.close()

    loop = asyncio.get_running_loop()
    return loop.call_later(timeout, _timeout, queue)
