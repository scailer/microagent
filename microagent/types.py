from typing import Callable, Literal, Protocol


PeriodicFunc = Callable[[], None]
HookFunc = Callable[[], None]
HookLabel = Literal['server', 'pre_start', 'post_start', 'pre_stop']
ReceiverFunc = Callable[..., None | int | str]
ConsumerFunc = Callable[..., None | int | str]
BoundKey = tuple[str, ...]


class BusProtocol(Protocol):
    uid: str
    dsn: str
    prefix: str

#    def __getattr__(self, name: str) -> BoundSignal:
#        ...
