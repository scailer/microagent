from typing import Dict, Callable, Union, TYPE_CHECKING
from dataclasses import dataclass
import ujson

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
    _queues = {}  # type: Dict[str, Queue]

    def __post_init__(self):
        self._queues[self.name] = self

    def __repr__(self) -> str:
        return f'<Queue {self.name}>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Queue):
            return NotImplemented
        return self.name == other.name

    @classmethod
    def get(cls, name: str) -> 'Queue':
        ''' Get the queue instance by name '''
        try:
            return cls._queues[name]
        except KeyError:
            raise QueueNotFound(f'No such queue {name}')

    @classmethod
    def get_all(cls) -> Dict[str, 'Queue']:
        ''' All registered queues '''
        return cls._queues

    def serialize(self, data: dict) -> str:
        '''
            Data serializing method

            :param data: dict of transfered data
        '''
        try:
            return ujson.dumps(data)
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc)

    def deserialize(self, data: str) -> dict:
        '''
            Data deserializing method

            :param data: serialized transfered data
        '''
        try:
            return ujson.loads(data)
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc)


@dataclass(frozen=True)
class Consumer:
    agent: 'MicroAgent'
    handler: Callable
    queue: Queue
    timeout: Union[int, float]
    options: dict

    def __repr__(self) -> str:
        name = getattr(self.handler, '__name__', 'unknown')
        return f'<Consumer {name} of {self.agent} for {self.queue}>'
