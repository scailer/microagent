import asyncio


async def _wrap(self, func, *args, **kwargs):
    response = None

    try:
        response = func(self, *args, **kwargs)
    except Exception as e:
        self.log.fatal('Periodic Exception: {}'.format(e), exc_info=True)

    if asyncio.iscoroutine(response):
        try:
            response = await asyncio.wait_for(response, func._timeout)
        except asyncio.TimeoutError:
            self.log.fatal('TimeoutError: {}'.format(func.__qualname__))
        except Exception as e:
            self.log.fatal('Periodic Exception: {}'.format(e), exc_info=True)

    self._loop.call_later(
        func._period,
        lambda *args, **kwargs: asyncio.ensure_future(_wrap(*args, **kwargs)),
        self, func, *args, **kwargs)


def periodic(**kwargs):
    '''
        Decorator for periodical task for Agent object

        def setup(self, cfg):
            self._loop.call_later(1, self.periodic_handler)  # initialize cycle

        @periodic(period=3, timeout=10)  # in seconds
        async def periodic_handler(self, **kwargs):
            print('PCALL')
    '''

    def _decorator(func):
        func._period = kwargs.get('period', 1)
        func._timeout = kwargs.get('timeout', 1)

        def _call(self, *args, **kwargs):
            ret = _wrap(self, func, *args, **kwargs)
            asyncio.ensure_future(ret, loop=self._loop)

        _call.test_run = func
        _call.origin = func
        _call._start_after = kwargs.get('start_after')
        return _call

    return _decorator
