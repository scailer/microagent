import unittest
from microagent.bus import AbstractSignalBus


class Bus(AbstractSignalBus):
    async def send(self, channel: str, message: str):
        pass

    async def bind(self, channel: str):
        pass

    def receiver(self, *args, **kwargs):
        pass


class TestBus(unittest.TestCase):
    def test_init(self):
        dsn = 'redis://localhost'
        bus = Bus(dsn)
        self.assertEqual(bus.dsn, dsn)
