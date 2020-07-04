'''
Prepared bus and broker mocks for testing based on unittest.mock.AsyncMock

.. code-block:: python

    from microagent.tools.mocks import BusMock, BrokerMock

    agent = Agent(bus=BusMock(), broker=BrokerMock())

    agent.bus.user_created.send.assert_called()
    agent.bus.user_created.call.assert_called()

    agent.broker.mailing.send.assert_called()
    agent.broker.mailing.length.assert_called()
'''
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
        self.bind_receiver = AsyncMock()
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
