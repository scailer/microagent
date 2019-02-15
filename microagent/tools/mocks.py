import unittest
import asynctest


class BoundSignalMock:
    send = asynctest.CoroutineMock()
    call = asynctest.CoroutineMock()


class BusMock:
    def __init__(self):
        self._stuff = {}
        self.bind_signal = asynctest.CoroutineMock()
        self.send = asynctest.CoroutineMock()
        self.call = asynctest.CoroutineMock()

    def __getattr__(self, name):
        self._stuff[name] = self._stuff.get(name, BoundSignalMock())
        return self._stuff[name]
