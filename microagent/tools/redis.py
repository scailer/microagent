import logging
import asyncio

from typing import Callable
from datetime import datetime
from collections import defaultdict


class RedisBrokerMixin:
    WAIT_TIME: int = 15
    BIND_TIME: float = 1
    ROLLBACK_ATTEMPTS: int = 3

    log: logging.Logger
    send: Callable
    _bindings: dict
    _rollbacks: dict

    def __init__(self, dsn: str, logger: logging.Logger = None) -> None:
        super().__init__(dsn, logger)  # type: ignore
        self.transport = None
        self._rollbacks = defaultdict(lambda: 0)

    async def new_connection(self):
        return NotImplemented

    async def bind(self, name: str) -> None:
        _loop = asyncio.get_running_loop()
        _loop.call_later(self.BIND_TIME, lambda: asyncio.create_task(self._wait(name)))

    async def _wait(self, name: str) -> None:
        transport = await self.new_connection()
        while True:
            data = await transport.blpop(name, self.WAIT_TIME)
            if data:
                _, data = data
                asyncio.create_task(self._handler(name, data))

    async def rollback(self, name: str, data: str):
        _hash = str(hash(name)) + str(hash(data))
        attempt = self._rollbacks[_hash]

        if attempt > self.ROLLBACK_ATTEMPTS:
            self.log.error('Rollback limit exceeded on queue "%s" with data: %s', name, data)
            return

        self.log.warning('Back message to queue "%s" attempt %d', name, attempt)

        _loop = asyncio.get_running_loop()
        _loop.call_later(attempt ** 2, lambda: asyncio.create_task(self.send(name, data)))

        self._rollbacks[_hash] += 1

    async def _handler(self, name: str, data: str):
        consumer = self._bindings[name]
        _data = consumer.queue.deserialize(data)  # type: dict

        try:
            response = consumer.handler(**_data)
        except Exception:
            self.log.error('Call %s failed', consumer.queue.name, exc_info=True)
            await self.rollback(consumer.queue.name, data)
            return

        if asyncio.iscoroutine(response):
            timer = datetime.now().timestamp()

            try:
                response = await asyncio.wait_for(response, consumer.timeout)
            except asyncio.TimeoutError:
                self.log.error('TimeoutError: %s %.2f', consumer,
                    datetime.now().timestamp() - timer)
                await self.rollback(name, data)
            except Exception:
                self.log.error('Call %s failed', consumer.queue.name, exc_info=True)
                await self.rollback(consumer.queue.name, data)
