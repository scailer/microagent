import unittest
from unittest.mock import Mock
from microagent.agent import MicroAgent


class TestAgent(unittest.TestCase):
    def test_init(self):
        ma = MicroAgent(Mock())
        self.assertEqual(ma.settings, {})
        self.assertEqual(ma._periodic_tasks, [])
        self.assertEqual(ma.received_signals, {})

    def test_info(self):
        ma = MicroAgent(Mock())
        self.assertTrue(bool(ma.info()))
