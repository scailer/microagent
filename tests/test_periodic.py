import asyncio
import unittest
import asynctest
import time
from microagent.periodic_task import _wrap, _periodic, _cron


async def test_wrap():
    obj = unittest.mock.MagicMock()
    foo = asynctest.CoroutineMock(_timeout=1)
    await _wrap(obj, foo)
    obj.log.fatal.assert_not_called()
    foo.assert_called()


async def test_wrap_exp():
    obj = unittest.mock.MagicMock()
    foo = asynctest.CoroutineMock(_timeout=1)
    foo.side_effect = Exception()
    await _wrap(obj, foo)
    obj.log.fatal.assert_called()
    foo.assert_called()


async def test_wrap_sync_exp():
    obj = unittest.mock.MagicMock()
    foo = unittest.mock.Mock(_timeout=1)
    foo.side_effect = Exception()
    await _wrap(obj, foo)
    obj.log.fatal.assert_called()
    foo.assert_called()


async def test_wrap_timeout():
    obj = unittest.mock.MagicMock()
    foo = asynctest.CoroutineMock(_timeout=.001, __qualname__='qname')

    async def _foo(*args, **kwargs):
        await asyncio.sleep(1)
    foo.side_effect = _foo

    await _wrap(obj, foo)
    obj.log.fatal.assert_called()
    foo.assert_called()


async def test_periodic():
    obj = unittest.mock.MagicMock(_loop=asyncio.get_event_loop())
    foo = asynctest.CoroutineMock(_timeout=.001, _period=.01)
    await _periodic(obj, foo)
    foo.assert_called()
    assert foo.call_count == 1
    await asyncio.sleep(.013)
    assert foo.call_count == 2
    await asyncio.sleep(.013)
    assert foo.call_count == 3


async def test_cron():
    _croniter = unittest.mock.Mock()
    _croniter.get_next = lambda x: time.time() + .01
    obj = unittest.mock.MagicMock(_loop=asyncio.get_event_loop())
    foo = asynctest.CoroutineMock(_timeout=.001, _croniter=_croniter)
    await _cron(obj, foo)
    foo.assert_called()
    assert foo.call_count == 1
    await asyncio.sleep(.013)
    assert foo.call_count == 2
    await asyncio.sleep(.013)
    assert foo.call_count == 3
