from typing import List, Dict, Callable, Union
from dataclasses import dataclass
import ujson


class SignalException(Exception):
    ''' Base signal exception '''
    pass


@dataclass(frozen=True)
class Signal:
    '''
        Signal instance is descriptional entity based on redis channel.

        Format of channel name:
            <prefix>:<name>:<sender>#<signal_id>

        prefix - global channel filter
        name - signal identificator
        sender - identificator of app wich send this signal
        signal_id - identificator of signal (optional)

        some_signal = Signal(
            providing_args=['some_arg'],
            name='some_signal')

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

    def make_channel_name(self, channel_prefix: str, sender: str = '*') -> str:
        return f'{channel_prefix}:{self.name}:{sender}'

    def serialize(self, data: dict) -> str:
        return ujson.dumps(data)

    def deserialize(self, data: str) -> dict:
        return ujson.loads(data)

    @classmethod
    def get(cls, name: str) -> 'Signal':
        ''' Get signal instance by name '''
        try:
            return cls._signals[name]
        except KeyError:
            raise SignalException(f'No such signal {name}')

    @classmethod
    def get_all(cls) -> Dict[str, 'Signal']:
        return cls._signals


@dataclass(frozen=True)
class Receiver:
    agent: 'microagent.MicroAgent'
    handler: Callable
    signal: Signal
    timeout: Union[int, float]

    def __repr__(self) -> str:
        return f'<Receiver {self.handler.__name__} of {self.agent} for {self.signal}>'
