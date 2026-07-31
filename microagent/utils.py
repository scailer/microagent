import asyncio

from collections.abc import Callable


class IterQueue(asyncio.Queue):
    ''' Queue as async generator '''

    def __aiter__(self) -> 'IterQueue':
        return self

    async def __anext__(self) -> dict[str, int | str | None]:
        try:
            value = await self.get()
            self.task_done()
            return value

        except asyncio.CancelledError as err:
            raise StopAsyncIteration from err


def make_bound_key(func: Callable) -> tuple[str, ...]:
    return func.__module__, *func.__qualname__.split('.')


def raise_timeout(timeout: float) -> None:
    ''' Interupt current corutine by timer '''

    def _timeout(task: asyncio.Task) -> None:
        if task._fut_waiter and not task._fut_waiter.cancelled():  # type: ignore[attr-defined]
            task.cancel()

    asyncio.get_event_loop().call_later(timeout, _timeout, asyncio.current_task())  # type: ignore[arg-type]
