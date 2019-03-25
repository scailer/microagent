import inspect
from functools import partial, wraps
from collections import defaultdict
from inspect import getmembers, ismethod


def on(*hooks: str):
    '''
        Bind to internal event
    '''

    def _decorator(func):
        func.__hook__ = [f'on_{hook}' for hook in hooks]
        return func

    return _decorator


def hook_decorator(hook):
    def decorator(method):
        if not any([
            f'on_pre_{method.__name__}' in hook.bindings,
            f'on_error_{method.__name__}' in hook.bindings,
            f'on_post_{method.__name__}' in hook.bindings
        ]):
            return method

        @wraps(method)
        async def wrapper(*args, **kwargs):
            context = {'args': args, 'kwargs': kwargs}

            if f'on_pre_{method.__name__}' in hook.bindings:
                await hook.call_hook(f'on_pre_{method.__name__}', context=context)

            try:
                response = method(*args, **kwargs)

                if inspect.isawaitable(response):
                    response = await response

            except Exception as exc:
                if f'on_error_{method.__name__}' in hook.bindings:
                    await hook.call_hook(f'on_error_{method.__name__}', exc, context=context)
                raise

            if f'on_post_{method.__name__}' in hook.bindings:
                await hook.call_hook(f'on_post_{method.__name__}', response, context=context)

            return response
        return wrapper
    return decorator


class Hooks:
    '''
        pre|error|post:method
    '''
    def __init__(self, agent):
        binded_methods = defaultdict(list)

        for _, method in getmembers(agent, ismethod):
            if hasattr(method, '__hook__'):
                for hook_name in method.__hook__:
                    binded_methods[hook_name].append(method)

        self._bindings = dict(binded_methods)

    @property
    def bindings(self):
        return self._bindings

    @property
    def decorate(self):
        return hook_decorator(self)

    async def call_hook(self, name, *args, **kwargs):
        methods = self._bindings.get(name)
        if methods:
            for method in methods:
                response = method(*args, **kwargs)
                if inspect.isawaitable(response):
                    await response

    def __getattr__(self, name: str):
        if name.startswith('on_'):
            return partial(self.call_hook, name)

        return getattr(super(), name)
