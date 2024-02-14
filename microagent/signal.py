import json

from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, ClassVar, TypedDict

from .abc import BoundKey, ReceiverFunc


if TYPE_CHECKING:
    from .agent import MicroAgent


class SignalException(Exception):
    ''' Base signal exception '''
    pass


class SignalNotFound(SignalException):
    pass


class SerializingError(SignalException):
    pass


@dataclass(slots=True, frozen=True)
class Signal:
    '''
        Dataclass (declaration) for a signal entity with a unique name.
        Each instance registered at creation.
        Usually, you don't need to work directly with the Signal-class.

        .. attribute:: name

            String, signal name, project-wide unique, `[a-z_]+`

        .. attribute:: providing_args

            All available and required parameters of message, can be simple list
            of argument names, or dictionary with declared types for each argument.
            If types declared, will be enabled soft type checking (warning log)
            for input data in runtime. Type checking works in `bus.send`,
            `bus.call` and on receiving signals. Supported only json-types:
            string, number, boolean, array, object, null.


        Declaration with config-file (signals.json).

        .. code-block:: json

            {
                "signals": [
                    {"name": "started", "providing_args": []},
                    {"name": "user_created", "providing_args": ["user_id"]},
                    {"name": "typed_signal", "providing_args": {
                        "uuid": "string",
                        "code": ["number", "null"],
                        "flag": "boolean",
                        "ids": "array"
                    }}
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
    providing_args: list[str]
    type_map: dict[str, tuple[type, ...]] | None = None

    _signals: ClassVar[dict[str, 'Signal']] = {}
    _jsonlib: ClassVar[ModuleType] = json

    def __post_init__(self) -> None:
        self._signals[self.name] = self

    def __repr__(self) -> str:
        return f'<Signal {self.name}>'

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Signal):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return id(self)

    @classmethod
    def set_jsonlib(cls, jsonlib: ModuleType) -> None:
        cls._jsonlib = jsonlib

    @classmethod
    def get(cls, name: str) -> 'Signal':
        ''' Get the signal instance by name '''
        try:
            return cls._signals[name]
        except KeyError as exc:
            raise SignalNotFound(f'No such signal {name}') from exc

    @classmethod
    def get_all(cls) -> dict[str, 'Signal']:
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
            return self._jsonlib.dumps(data)
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc) from exc

    def deserialize(self, data: str) -> dict:
        '''
            Data deserializing method

            :param data: serialized transfered data
        '''
        try:
            return dict(self._jsonlib.loads(data))
        except (ValueError, TypeError, OverflowError) as exc:
            raise SerializingError(exc) from exc


class ReceiverArgs(TypedDict):
    signal: Signal
    timeout: float


@dataclass(slots=True, frozen=True)
class Receiver:
    agent: 'MicroAgent'
    handler: ReceiverFunc
    signal: Signal
    timeout: float

    _register: ClassVar[dict[BoundKey, ReceiverArgs]] = {}

    @property
    def key(self) -> str:
        return self.handler.__qualname__

    def __repr__(self) -> str:
        return f'<Receiver {self.handler.__name__} of {self.agent} for {self.signal}>'
