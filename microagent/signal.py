from typing import List, Callable
from collections import namedtuple
import ujson


LookupKey = namedtuple('LookupKey', ['mod', 'name', 'id'])


def _make_id(target):
    if isinstance(target, str):
        return target
    if hasattr(target, 'im_func'):
        return id(target.im_self), id(target.im_func)
    return id(target)


def _make_lookup_key(receiver, sender):
    return LookupKey(mod=receiver.__module__, name=receiver.__qualname__, id=_make_id(sender))


class SignalException(Exception):
    ''' Base signal exception '''
    pass


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

    _signals: dict = {}
    receivers: list

    def __new__(cls, *args, **kwargs):
        name = kwargs.get('name', args[0] if args else None)
        return cls._signals[name] if name in cls._signals else super().__new__(cls)

    def __init__(self, name: str, providing_args: List[str], serializer=None):
        if name in self._signals:
            return

        self.name = name
        self.providing_args = providing_args
        self.serializer = serializer or ujson
        self._signals[name] = self
        self.receivers = []

    def __repr__(self):
        return f'<Signal {self.name}>'

    def __eq__(self, other):
        return self.name == other.name

    def connect(self, receiver: Callable, sender: str = None) -> None:
        ''' Bind method to signal '''
        lookup_key = LookupKey(
            mod=receiver.__module__, name=receiver.__qualname__, id=_make_id(sender))

        for key, _ in self.receivers:
            if (key.mod, key.name) == (lookup_key.mod, lookup_key.name):
                break
        else:
            self.receivers.append((lookup_key, receiver))

    def make_channel_name(self, channel_prefix: str, sender: str = '*') -> str:
        ''' Make channel name '''
        return '{}:{}:{}'.format(channel_prefix, self.name, _make_id(sender))

    def serialize(self, data: dict) -> str:
        return self.serializer.dumps(data)

    def deserialize(self, data: str) -> dict:
        return self.serializer.loads(data)

    @classmethod
    def get(cls, name: str):
        ''' Get signal instance by name '''
        try:
            return cls._signals[name]
        except KeyError:
            raise SignalException(f'No such signal {name}')

    @classmethod
    def get_all(cls):
        return cls._signals
