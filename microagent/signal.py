from typing import List, Dict, Callable, Union, TYPE_CHECKING
from dataclasses import dataclass
import ujson

if TYPE_CHECKING:
    from .agent import MicroAgent


class SignalException(Exception):
    ''' Base signal exception '''
    pass


class SignalNotFound(SignalException):
    pass


class SerializingError(SignalException):
    pass


@dataclass(frozen=True)
class Signal:
    '''
        Dataclass (declaration) for a signal entity with a unique name.
        Each instance registered at creation.
        Usually, you don't need to work directly with the Signal-class.

        .. attribute:: name

            String, signal name, project-wide unique, `[a-z_]+`

        .. attribute:: providing_args

            List of strings, all available and required parameters of message

        .. attribute:: content_type

            String, content format, `json` by default


        Declaration with config-file (signals.json)

        .. code-block:: json

            {
                "signals": [
                    {"name": "started", "providing_args": []},
                    {"name": "user_created", "providing_args": ["user_id"]},
                ]
            }

        Manual declaration (not recommended)

        .. code-block:: python

            some_signal = Signal(
                name='some_signal',
                providing_args=['some_arg']
            )
    '''

    name: str
    providing_args: List[str]
    content_type: str = 'json'
    _signals = {}  # type: Dict[str, Signal]

    def __post_init__(self) -> None:
        self._signals[self.name] = self

    def __repr__(self) -> str:
        return f'<Signal {self.name}>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Signal):
            return NotImplemented
        return self.name == other.name

    @classmethod
    def get(cls, name: str) -> 'Signal':
        ''' Get the signal instance by name '''
        try:
            return cls._signals[name]
        except KeyError:
            raise SignalNotFound(f'No such signal {name}')

    @classmethod
    def get_all(cls) -> Dict[str, 'Signal']:
        ''' All registered signals '''
        return cls._signals

    def make_channel_name(self, channel_prefix: str, sender: str = '*') -> str:
        '''
            Construct a channel name by the signal description

            :param channel_prefix: prefix, often project name
            :param sender: name of signal sender
        '''
        return f'{channel_prefix}:{self.name}:{sender}'

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
class Receiver:
    agent: 'MicroAgent'
    handler: Callable
    signal: Signal
    timeout: Union[int, float]

    @property
    def key(self) -> str:
        return self.handler.__qualname__

    def __repr__(self) -> str:
        return f'<Receiver {self.handler.__name__} of {self.agent} for {self.signal}>'
