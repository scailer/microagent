from typing import List, Dict
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

    def __post_init__(self):
        self._signals[self.name] = self

    def __repr__(self):
        return f'<Signal {self.name}>'

    def __eq__(self, other):
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
