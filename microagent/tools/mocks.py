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
from typing import Any
from unittest.mock import AsyncMock, MagicMock, _safe_super  # type: ignore[attr-defined]

from microagent.broker import AbstractQueueBroker
from microagent.bus import AbstractSignalBus


class BoundSignalMock:
    def __init__(self) -> None:
        self.send = AsyncMock()
        self.call = AsyncMock()


class BusMock(MagicMock):
    def __init__(self, /, *args: Any, **kw: Any) -> None:
        self._mock_set_magics()  # type: ignore[operator]
        _safe_super(BusMock, self).__init__(*args, **kw)
        self._mock_add_spec(AbstractSignalBus, spec_set=False)
        self._mock_set_magics()  # type: ignore[operator]

        self._stuff: dict[str, BoundSignalMock] = {}
        self.bind_receiver = AsyncMock()
        self.bind_signal = AsyncMock()
        self.send = AsyncMock()
        self.call = AsyncMock()

    def __str__(self) -> str:
        return f'<BusMock id={id(self)}'

    def __getattr__(self, name: str) -> BoundSignalMock:
        if name.startswith('_') or name in {'bind_signal', 'send', 'call'}:
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundSignalMock())
        return self._stuff[name]


class BoundQueueMock:
    def __init__(self) -> None:
        self.send = AsyncMock()
        self.length = AsyncMock()
        self.declare = AsyncMock()


class BrokerMock(MagicMock):
    def __init__(self, /, *args: Any, **kw: Any) -> None:
        self._mock_set_magics()  # type: ignore[operator]
        _safe_super(BrokerMock, self).__init__(*args, **kw)
        self._mock_add_spec(AbstractQueueBroker, spec_set=False)
        self._mock_set_magics()  # type: ignore[operator]

        self._stuff: dict[str, BoundQueueMock] = {}
        self.bind_consumer = AsyncMock()
        self.send = AsyncMock()
        self.dsn = ''
        self.uid = ''

    def __str__(self) -> str:
        return f'<BrokerMock id={id(self)}'

    def __getattr__(self, name: str) -> BoundQueueMock:
        if name.startswith('_') or name in {'bind_consumer', 'send'}:
            return super().__getattr__(name)
        self._stuff[name] = self._stuff.get(name, BoundQueueMock())
        return self._stuff[name]
