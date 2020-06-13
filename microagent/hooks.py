'''
Internal hooks
'''

import inspect
from dataclasses import dataclass
from collections import defaultdict
from typing import Callable, Dict, List, Iterable, Awaitable, TYPE_CHECKING

if TYPE_CHECKING:
    from .agent import MicroAgent


@dataclass(frozen=True)
class Hook:
    agent: 'MicroAgent'
    handler: Callable
    label: str


class Hooks:
    '''
    Internal hooks
    '''

    binds: Dict[str, List[Hook]]

    def __init__(self, hooks: Iterable[Hook]):
        self.binds = defaultdict(list)

        for hook in hooks:
            self.binds[hook.label].append(hook)

    @property
    def servers(self) -> Iterable[Callable]:
        return (hook.handler for hook in self.binds['server'])

    def pre_start(self) -> Awaitable:
        return self.call('pre_start')

    def post_start(self) -> Awaitable:
        return self.call('post_start')

    def pre_stop(self) -> Awaitable:
        return self.call('pre_stop')

    async def call(self, label: str) -> None:
        for hook in self.binds[label]:
            response = hook.handler()
            if inspect.isawaitable(response):
                await response
