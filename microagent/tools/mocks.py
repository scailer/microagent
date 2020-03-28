from unittest.mock import AsyncMock, MagicMock
from microagent.bus import AbstractSignalBus
from microagent.broker import AbstractQueueBroker


class BoundSignalMock:
    def __init__(self):
        self.send = AsyncMock()
        self.call = AsyncMock()


class BusMock(MagicMock):
    def __init__(self):
        super().__init__(spec=AbstractSignalBus)
        self._stuff = {}
        self.bind_signal = AsyncMock()
        self.send = AsyncMock()
        self.call = AsyncMock()
        self.__str__ = lambda x: self.__class__.__name__

    def __getattr__(self, name):
        if name.startswith('_') or name in ('bind_signal', 'send', 'call'):
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundSignalMock())
        return self._stuff[name]


class BoundQueueMock:
    def __init__(self):
        self.send = AsyncMock()
        self.length = AsyncMock()
        self.declare = AsyncMock()


class BrokerMock(MagicMock):
    def __init__(self):
        super().__init__(spec=AbstractQueueBroker)
        self._stuff = {}
        self.bind_consumer = AsyncMock()
        self.send = AsyncMock()
        self.__str__ = lambda x: self.__class__.__name__

    def __getattr__(self, name):
        if name.startswith('_') or name in ('bind_consumer', 'send'):
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundQueueMock())
        return self._stuff[name]
