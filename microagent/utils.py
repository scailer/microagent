import asyncio


class IterQueue(asyncio.Queue):
    ''' Queue as async generator '''

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            value = await self.get()
            self.task_done()
            return value

        except asyncio.CancelledError:
            raise StopAsyncIteration


def raise_timeout(timeout: float):
    ''' Interupt current corutine by timer '''

    def _timeout(task):
        if task._fut_waiter and not task._fut_waiter.cancelled():
            task.cancel()

    asyncio.get_event_loop().call_later(timeout, _timeout, asyncio.current_task())
