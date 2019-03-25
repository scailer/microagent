from pathlib import Path
from microagent import (MicroAgent, Signal, Queue, receiver, consumer,  # noqa
    periodic, cron, load_stuff, load_signals, load_queues, __version__)  # noqa

source = 'file://' + str(Path(__file__).parent / 'stuff.json')
source_url = 'https://gist.githubusercontent.com/scailer/ee0baed54a9444f328c9d0fd4ed84bed/raw/dfae17935813ee6a0ef0d441cbebc19572f8488c/stuff.json'  # noqa


def test_load_from_file():
    signals, queues = load_stuff(source)
    assert len(signals) == 2
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert queues.test_queue.name == 'test_queue'


def test_load_signals():
    signals = load_signals(source)
    assert len(signals) == 2
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'


def test_load_queues():
    queues = load_queues(source)
    assert len(queues) == 1
    assert queues.test_queue.name == 'test_queue'


def test_load_from_url():
    signals, queues = load_stuff(source_url)
    assert len(signals) == 2
    assert len(queues) == 1
    assert signals.test_signal.name == 'test_signal'
    assert signals.else_signal.name == 'else_signal'
    assert queues.test_queue.name == 'test_queue'
