import asynctest
from microagent.bus import AbstractSignalBus
from microagent.broker import AbstractQueueBroker


class BoundSignalMock:
    def __init__(self):
        self.send = asynctest.CoroutineMock()
        self.call = asynctest.CoroutineMock()


class BusMock(asynctest.MagicMock):
    def __init__(self):
        super().__init__(spec=AbstractSignalBus)
        self._stuff = {}
        self.bind_signal = asynctest.CoroutineMock()
        self.send = asynctest.CoroutineMock()
        self.call = asynctest.CoroutineMock()
        self.__str__ = lambda x: self.__class__.__name__

    def __getattr__(self, name):
        if name.startswith('_') or name in ('bind_signal', 'send', 'call'):
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundSignalMock())
        return self._stuff[name]


class BoundQueueMock:
    def __init__(self):
        self.send = asynctest.CoroutineMock()
        self.length = asynctest.CoroutineMock()
        self.declare = asynctest.CoroutineMock()


class BrokerMock(asynctest.MagicMock):
    def __init__(self):
        super().__init__(spec=AbstractQueueBroker)
        self._stuff = {}
        self.bind_consumer = asynctest.CoroutineMock()
        self.send = asynctest.CoroutineMock()
        self.__str__ = lambda x: self.__class__.__name__

    def __getattr__(self, name):
        if name.startswith('_') or name in ('bind_consumer', 'send'):
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundQueueMock())
        return self._stuff[name]
