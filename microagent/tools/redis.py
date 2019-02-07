import logging
import asyncio

from typing import Optional
from datetime import datetime
from collections import defaultdict


class RedisBrokerMixin:
    WAIT_TIME = 15
    ROLLBACK_ATTEMPTS = 3

    def __init__(self, dsn: str, logger: Optional[logging.Logger] = None):
        super().__init__(dsn, logger)
        self.transport = None
        self._rollbacks = defaultdict(lambda: 0)

    async def new_connection(self):
        return NotImplemented

    async def bind(self, name, handler):
        if name in self._bindings:
            self._bindings[name].append(handler)
        else:
            self._bindings[name] = [handler]
            self._loop.call_later(1, lambda: asyncio.ensure_future(self._wait(name)))

    async def _wait(self, name):
        transport = await self.new_connection()
        while True:
            data = await transport.blpop(name, self.WAIT_TIME)
            if data:
                _, data = data
                asyncio.ensure_future(self._handler(name, data))

    async def rollback(self, name, data):
        _hash = str(hash(name)) + str(hash(data))
        attempt = self._rollbacks[_hash]

        if attempt > self.ROLLBACK_ATTEMPTS:
            self.log.error('Rollback limit exceeded on queue "%s" with data: %s', name, data)
            return

        self.log.warning('Back message to queue "%s" attempt %d', name, attempt)
        self._loop.call_later(
            attempt ** 2, lambda: asyncio.ensure_future(self.send(name, data)))
        self._rollbacks[_hash] += 1

    async def _handler(self, name, data):
        for handler in self._bindings[name]:
            _data = handler.queue.deserialize(data)
            try:
                response = handler(**_data)
            except Exception:
                self.log.error('Call %s failed', handler.queue.name, exc_info=True)
                await self.rollback(handler.queue.name, data)
                return

            if asyncio.iscoroutine(response):
                timer = datetime.now().timestamp()

                try:
                    response = await asyncio.wait_for(response, handler.timeout)
                except asyncio.TimeoutError:
                    self.log.fatal('TimeoutError: %s %.2f', handler.__qualname__,
                        datetime.now().timestamp() - timer)
                    await self.rollback(name, data)
                except Exception:
                    self.log.error('Call %s failed', handler.queue.name, exc_info=True)
                    await self.rollback(handler.queue.name, data)
