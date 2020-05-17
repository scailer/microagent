from typing import List, Dict, Callable, Union
from dataclasses import dataclass
import ujson


class QueueException(Exception):
    ''' Base signal exception '''
    pass


class QueueNotFound(QueueException):
    pass


class SerializingError(QueueException):
    pass


@dataclass(frozen=True)
class Queue:
    name: str
    _queues = {}  # type: Dict[str, Queue]

    def __post_init__(self):
        self._queues[self.name] = self

    def __repr__(self) -> str:
        return f'<Queue {self.name}>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Queue):
            return NotImplemented
        return self.name == other.name

    def serialize(self, data: dict) -> str:
        try:
            return ujson.dumps(data)
        except (ValueError, TypeError) as exc:
            raise SerializingError(exc)

    def deserialize(self, data: str) -> dict:
        try:
            return ujson.loads(data)
        except (ValueError, TypeError) as exc:
            raise SerializingError(exc)

    @classmethod
    def get(cls, name: str) -> 'Queue':
        ''' Get signal instance by name '''
        try:
            return cls._queues[name]
        except KeyError:
            raise QueueNotFound(f'No such queue {name}')

    @classmethod
    def get_all(cls) -> Dict[str, 'Queue']:
        return cls._queues


@dataclass(frozen=True)
class Consumer:
    agent: 'microagent.MicroAgent'
    handler: Callable
    queue: Queue
    timeout: Union[int, float]
    options: dict

    def __repr__(self) -> str:
        return f'<Consumer {self.handler.__name__} of {self.agent} for {self.queue}>'
