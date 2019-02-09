import asyncio
import unittest
import asynctest
from unittest.mock import Mock
from pathlib import Path
from microagent import (MicroAgent, Signal, Queue, receiver, consumer,
                        periodic, cron, load_stuff, load_signals, load_queues)

source = 'file://' + str(Path(__file__).parent / 'stuff.json')
source_url = 'https://gist.githubusercontent.com/scailer/ee0baed54a9444f328c9d0fd4ed84bed/raw/dfae17935813ee6a0ef0d441cbebc19572f8488c/stuff.json'


class TestAgent(asynctest.TestCase):
    def test_load_from_file(self):
        signals, queues = load_stuff(source)
        self.assertEqual(len(signals), 2)
        self.assertEqual(len(queues), 1)
        self.assertEqual(signals.test_signal.name, 'test_signal')
        self.assertEqual(signals.else_signal.name, 'else_signal')
        self.assertEqual(queues.test_queue.name, 'test_queue')

    def test_load_signals(self):
        signals = load_signals(source)
        self.assertEqual(len(signals), 2)
        self.assertEqual(signals.test_signal.name, 'test_signal')
        self.assertEqual(signals.else_signal.name, 'else_signal')

    def test_load_queues(self):
        queues = load_queues(source)
        self.assertEqual(len(queues), 1)
        self.assertEqual(queues.test_queue.name, 'test_queue')

    def test_load_from_url(self):
        signals, queues = load_stuff(source_url)
        self.assertEqual(len(signals), 2)
        self.assertEqual(len(queues), 1)
        self.assertEqual(signals.test_signal.name, 'test_signal')
        self.assertEqual(signals.else_signal.name, 'else_signal')
        self.assertEqual(queues.test_queue.name, 'test_queue')
