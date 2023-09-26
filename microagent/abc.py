import logging

from typing import Any, Awaitable, Callable, Literal, Protocol


HookLabel = Literal['server', 'pre_start', 'post_start', 'pre_stop']
HookFunc = Callable[[], None | Awaitable[None]]
PeriodicFunc = Callable[[], Awaitable[None]]
ReceiverFunc = Callable[..., Awaitable[None | int | str]]
ConsumerFunc = Callable[..., Awaitable[None]]
BoundKey = tuple[str, ...]


class SignalProtocol(Protocol):
    async def send(self, sender: str, **kwargs: Any) -> None:
        ...

    async def call(self, sender: str, *, timeout: int = 60, **kwargs: Any) -> int | str | None:
        ...


class BusProtocol(Protocol):
    dsn: str
    uid: str
    log: logging.Logger
    prefix: str

    def __getattr__(self, name: str) -> SignalProtocol:
        ...


class QueueProtocol(Protocol):
    async def send(self, message: dict, **options: Any) -> None:
        ...


class BrokerProtocol(Protocol):
    dsn: str
    uid: str
    log: logging.Logger

    def __getattr__(self, name: str) -> QueueProtocol:
        ...
