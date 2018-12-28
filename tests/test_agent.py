import unittest
import mock
from microagent.agent import MicroAgent


class TestAgent(unittest.TestCase):
    def test_init(self):
        ma = MicroAgent(mock.Mock())
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma._periodic_tasks, [])
        self.assertEqual(ma.received_signals, {})

    def test_info(self):
        ma = MicroAgent(mock.Mock())
        self.assertTrue(bool(ma.info()))
