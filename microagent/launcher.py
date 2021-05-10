'''
Configuration and launch MicroAgents with shipped launcher.
Configuration file is a python-file with 3 dictionaries:
AGENT, BUS and BROKER, where specified all settings.
Launcher can run microagents from one or several files.

.. code-block::  shell

    $ marun myproject.app1 myproject.app2


Each microagent is launched in a separate os process, and the launcher works
as a supervisor. All agents started by a single command are called a deployment
group and start/stop at the same time. If one of the agents stops, the launcher
stops the entire group.
'''

import os
import time
import signal
import asyncio
import argparse
import importlib
import logging
import multiprocessing

from typing import Optional, Iterator, Tuple, Dict, List, Any, TYPE_CHECKING
from itertools import chain
from functools import partial

if TYPE_CHECKING:
    from .agent import MicroAgent
    from .bus import AbstractSignalBus
    from .broker import AbstractQueueBroker


CFG_T = Tuple[str, Dict[str, Any]]

__all__ = (
    'GroupInterrupt',
    'load_configuration',
    'init_agent',
    'run'
)


MASTER_WATCHER_PERIOD = 5  # sec

logger = logging.getLogger('microagent.run')


class GroupInterrupt(SystemExit):
    pass


class ServerInterrupt(Exception):
    ''' Graceful server interruption '''
    pass


def load_configuration(config_path: str) -> Iterator[Tuple[str, CFG_T]]:
    '''
        Load configuration from module and prepare it for initializing agents.
        Returns list of unfolded configs for each agent.

    '''

    mod = importlib.import_module(config_path)

    _buses = dict(_configuration(getattr(mod, 'BUS', {})))
    _brokers = dict(_configuration(getattr(mod, 'BROKER', {})))

    for name, (backend, cfg) in _configuration(getattr(mod, 'AGENT', {})):
        cfg['bus'] = _buses.get(cfg.pop('bus', None))
        cfg['broker'] = _brokers.get(cfg.pop('broker', None))
        yield name, (backend, cfg)


def _configuration(data: Dict[str, Dict[str, str]]) -> Iterator[Tuple[str, CFG_T]]:
    for name, params in data.items():
        backend = params.pop('backend', None)
        if backend:
            yield name, (backend, params)


def init_agent(backend: str, cfg: Dict[str, Any]) -> 'MicroAgent':
    '''
        Import and load all using backends from config,
        initialize it and returns not started MicroAgent instance
    '''

    bus: Optional['AbstractSignalBus'] = None
    broker: Optional['AbstractQueueBroker'] = None
    _bus: Optional[CFG_T] = cfg.pop('bus', None)
    _broker: Optional[CFG_T] = cfg.pop('broker', None)

    if _bus:
        bus = _import(_bus[0])(**_bus[1])

    if _broker:
        broker = _import(_broker[0])(**_broker[1])

    return _import(backend)(bus=bus, broker=broker, **cfg)


def _import(path: str):
    mod = importlib.import_module('.'.join(path.split('.')[:-1]))
    return getattr(mod, path.split('.')[-1])


def _run_agent(name: str, backend: str, cfg: Dict[str, Any], parent_pid: int) -> None:
    '''
        Initialize MicroAgent instance from config and run it forever
        Contains handling for process control
    '''
    logger.info('Run Agent %s pid#%s', name, os.getpid())

    asyncio.run(run_agent(name, backend, cfg, parent_pid))

    logger.info('Stoped Agent %s pid#%s', name, os.getpid())


async def run_agent(name: str, backend: str, cfg: Dict[str, Any], parent_pid: int) -> None:
    loop = asyncio.get_event_loop()

    # Interrupt process when master shutdown
    loop.add_signal_handler(signal.SIGINT, partial(_interrupter, 'INT'))
    loop.add_signal_handler(signal.SIGTERM, partial(_interrupter, 'TERM'))

    # Check master & force break
    loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, parent_pid, loop)

    agent = init_agent(backend, cfg)

    try:
        await agent.start()  # wait when servers used

        while True:  # wait when no servers in agent
            logger.debug('Agent %s alive', name)
            await asyncio.sleep(3600)

    except (KeyboardInterrupt, GroupInterrupt, ServerInterrupt) as exc:
        logger.warning('Catch interrupt %s', exc)

    except Exception as exc:
        logger.exception('Catch error %s', exc)
        raise


def _interrupter(sig):
    logger.warning('Catch %s signal', sig)
    raise GroupInterrupt(sig)


def _master_watcher(pid: int, loop: asyncio.BaseEventLoop):
    loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, pid, loop)
    try:
        os.kill(pid, 0)  # check master process
    except ProcessLookupError:
        logger.warning('Parent process#%s closed, exiting...', pid)
        os._exit(os.EX_OK)  # noqa: W0212 hard break better than deattached pocesses


class AgentsManager:
    '''
        AgentsManager is a supervisor for launching and control group of microagents.
        When we run AgentsManager, it fork daemon process, strat microagent in the it,
        and wait when it finished or failed, then send SIGTERM for all other working processes.
    '''

    mp_ctx: multiprocessing.context.BaseContext
    processes: Dict[int, multiprocessing.process.BaseProcess]
    cfg: List[Tuple[str, CFG_T]]
    running: bool
    intlock: bool

    def __init__(self, cfg: List[Tuple[str, CFG_T]]) -> None:
        self.mp_ctx = multiprocessing.get_context()
        self.processes = {}
        self.cfg = cfg
        self.running = False
        self.intlock = False

        signal.signal(signal.SIGTERM, self._signal_cb)
        signal.signal(signal.SIGINT, self._signal_cb)

    def _signal_cb(self, signum: int, *args) -> None:
        signame = {2: 'INT', 15: 'TERM'}.get(signum, signum)
        logger.warning('Catch %s %s', signame, list(self.processes))
        self.running = False

    def start(self) -> None:
        ''' Starting and keep running unix-processes with agents '''

        self.running = True

        for name, _cfg in self.cfg:
            proc = self.mp_ctx.Process(
                target=_run_agent,
                name=name,
                args=(name, *_cfg),
                kwargs={'parent_pid': self.mp_ctx.current_process().pid},
                daemon=True
            )

            proc.start()

            if not proc.pid:
                logger.error('Fail starting %s', name)
                self.close()
                return

            self.processes[proc.pid] = proc

        logger.info('Agents started')

        while self.running:
            time.sleep(.01)

            if any(not p.is_alive() for p in self.processes.values()):
                self.running = False  # one finished - all finished

        logger.info('Agents stoped')
        self.close()

    def close(self):
        ''' Terminating dependent processes '''

        if self.intlock:  # Prevent duble kill
            logger.warning('Force kill locked')
            return

        self.intlock = True
        logger.warning('Force kill processes %s', list(self.processes))

        for process in self.processes.values():
            process.terminate()

        time.sleep(.1)
        logger.info('Forked processes killed')


def run():
    ''' Parse input and run '''

    parser = argparse.ArgumentParser(description='Run microagents')
    parser.add_argument('modules', metavar='MODULE_PATH', type=str, nargs='+',
        help='configuration modeles path')
    # parser.add_argument('--bind', metavar='HOST:POST', type=str, dest='bind',
    #     help='Bind duty interface')

    call_args = parser.parse_args()
    cfg = list(chain(*[load_configuration(module) for module in call_args.modules]))

    try:
        AgentsManager(cfg).start()
    finally:
        parser.exit(message="Exit\n")
