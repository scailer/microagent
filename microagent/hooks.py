'''
In practice, it is useful to be able to perform some actions before the microagent
starts working or after it stops. For this aim there are internal hooks that allow
you to run methods on pre_start, post_start, and pre_stop.

**pre_start** - is called before the microagent is ready to accept events and
consume messages. This ensures that handlers will be called when already
connections established with other services - databases, mail, logs;
initialized caches, objects, and so on.

**post_start** - called when the microagent has already started accepting events and
messages. It is can be useful for sending notifications to monitoring service and etc.

**pre_stop** - called when the microagent go shutdown. It can be useful for sending
notifications to the monitoring service, and so on.

**server** - "run forever" handler. If it crashes with exception microagent will be stopped.
If you are using a launcher from the library and server run forever,
it is important correctly to stop the servers with ServerInterrupt exception.

In addition, there is a special mechanism for running nested services. Methods
marked with the server decorator will be started in "run forever" mode. It's
allow provide endpoints for microagent, such as http, websocket, smtp or other.
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
