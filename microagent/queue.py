import json

from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, ClassVar, TypedDict

from .abc import BoundKey, ConsumerFunc


if TYPE_CHECKING:
    from .agent import MicroAgent


class QueueException(Exception):
    ''' Base queue exception '''
    pass


class QueueNotFound(QueueException):
    pass


class SerializingError(QueueException):
    pass


@dataclass(frozen=True)
class Queue:
    '''
        Dataclass (declaration) for a queue entity with a unique name.
        Each instance registered at creation.
        Usually, you don't need to work directly with the Queue-class.

        .. attribute:: name

            String, queue name, project-wide unique, `[a-z_]+`

        Declaration with config-file (queues.json)

        .. code-block:: json

            {
                "queues": [
                    {"name": "mailer"},
                    {"name": "pusher"},
                ]
            }

        Manual declaration (not recommended)

        .. code-block:: python

            some_queue = Queue(
                name='some_queue'
            )
    '''
    name: str

    _queues: ClassVar[dict[str, 'Queue']] = {}
    _jsonlib: ClassVar[ModuleType] = json

    def __post_init__(self) -> None:
        self._queues[self.name] = self

    def __repr__(self) -> str:
        return f'<Queue {self.name}>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Queue):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return id(self)

    @classmethod
    def set_jsonlib(cls, jsonlib: ModuleType) -> None:
        cls._jsonlib = jsonlib

    @classmethod
    def get(cls, name: str) -> 'Queue':
        ''' Get the queue instance by name '''
        try:
            return cls._queues[name]
        except KeyError as exc:
            raise QueueNotFound(f'No such queue {name}') from exc

    @classmethod
    def get_all(cls) -> dict[str, 'Queue']:
        ''' All registered queues '''
        return cls._queues

    def serialize(self, data: dict) -> str:
        '''
            Data serializing method

            :param data: dict of transfered data
        '''
        try:
            return self._jsonlib.dumps(data)
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc) from exc

    def deserialize(self, data: str | bytes) -> dict:
        '''
            Data deserializing method

            :param data: serialized transfered data
        '''
        try:
            return self._jsonlib.loads(data)
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc) from exc


class ConsumerArgs(TypedDict):
    queue: Queue
    timeout: float
    dto_class: type | None
    dto_name: str | None
    options: dict


@dataclass(frozen=True)
class Consumer:
    agent: 'MicroAgent'
    handler: ConsumerFunc
    queue: Queue
    timeout: float
    options: dict
    dto_class: type | None = None
    dto_name: str | None = None

    _register: ClassVar[dict[BoundKey, ConsumerArgs]] = {}

    def __repr__(self) -> str:
        name = getattr(self.handler, '__name__', 'unknown')
        return f'<Consumer {name} of {self.agent} for {self.queue}>'
