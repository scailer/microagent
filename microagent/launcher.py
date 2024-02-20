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

import argparse
import asyncio
import importlib
import logging
import multiprocessing
import os
import signal
import time

from collections.abc import Iterator
from functools import partial
from itertools import chain
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from .agent import MicroAgent
    from .broker import AbstractQueueBroker
    from .bus import AbstractSignalBus


CFG_T = tuple[str, dict[str, Any]]

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


def load_configuration(config_path: str) -> Iterator[tuple[str, CFG_T]]:
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


def _configuration(data: dict[str, dict[str, str]]) -> Iterator[tuple[str, CFG_T]]:
    for name, params in data.items():
        backend = params.pop('backend', None)
        if backend:
            yield name, (backend, params)


def init_agent(backend: str, cfg: dict[str, Any]) -> 'MicroAgent':
    '''
        Import and load all using backends from config,
        initialize it and returns not started MicroAgent instance
    '''

    bus: 'AbstractSignalBus' | None = None
    broker: 'AbstractQueueBroker' | None = None
    _bus: CFG_T | None = cfg.pop('bus', None)
    _broker: CFG_T | None = cfg.pop('broker', None)

    if _bus:
        bus = _import(_bus[0])(**_bus[1])

    if _broker:
        broker = _import(_broker[0])(**_broker[1])

    return _import(backend)(bus=bus, broker=broker, **cfg)


def _import(path: str) -> type:
    mod = importlib.import_module('.'.join(path.split('.')[:-1]))
    return getattr(mod, path.split('.')[-1])


def run_agent(name: str, backend: str, cfg: dict[str, Any]) -> None:
    '''
        Initialize MicroAgent instance from config and run it forever
        Contains handling for process control
    '''
    logger.info('AgentProc[%s]: run on pid#%s', name, os.getpid())

    asyncio.run(_run_agent(name, backend, cfg))

    logger.info('AgentProc[%s]: stoped pid#%s', name, os.getpid())


async def _run_agent(name: str, backend: str, cfg: dict[str, Any]) -> None:
    loop = asyncio.get_event_loop()

    # Interrupt process when master shutdown
    loop.add_signal_handler(signal.SIGINT, partial(_interrupter, name, 'INT'))
    loop.add_signal_handler(signal.SIGTERM, partial(_interrupter, name, 'TERM'))

    # Check master & force break
    loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, name)

    try:
        agent = init_agent(backend, cfg)
        await agent.start()  # wait when servers used

        while True:  # wait when no servers in agent
            logger.debug('AgentProc[%s]: alive', name)
            await asyncio.sleep(3600)

    except (KeyboardInterrupt, GroupInterrupt, ServerInterrupt) as exc:
        logger.warning('AgentProc[%s]: Catch interrupt %s', name, exc)

        for t in asyncio.all_tasks(loop=loop):
            t.cancel()

        await asyncio.sleep(.1)
        loop.stop()

    except Exception as exc:
        logger.exception('AgentProc[%s]: Catch error %s', name, exc)
        raise

    finally:
        raise SystemExit()


def _interrupter(name: str, sig: str) -> None:
    logger.warning('AgentProc[%s]: catch %s signal', name, sig)
    raise GroupInterrupt(sig)


def _master_watcher(name: str) -> None:
    asyncio.get_running_loop().call_later(MASTER_WATCHER_PERIOD, _master_watcher, name)

    if not (pid := getattr(multiprocessing.parent_process(), 'pid', None)):
        logger.warning('AgentProc[%s]: parent process not found, exiting...', name)
        raise SystemExit()

    try:
        os.kill(pid, 0)  # check master process
    except ProcessLookupError as exc:
        logger.warning('AgentProc[%s]: parent process#%s closed, exiting...', name, pid)
        raise SystemExit() from exc


class AgentsManager:
    '''
        AgentsManager is a supervisor for launching and control group of microagents.
        When we run AgentsManager, it fork daemon process, strat microagent in the it,
        and wait when it finished or failed, then send SIGTERM for all other working processes.
    '''

    mp_ctx: multiprocessing.context.DefaultContext
    processes: dict[int, multiprocessing.process.BaseProcess]
    cfg: list[tuple[str, CFG_T]]
    running: bool

    def __init__(self, cfg: list[tuple[str, CFG_T]]) -> None:
        self.mp_ctx = multiprocessing.get_context()
        self.processes = {}
        self.cfg = cfg
        self.running = False

        signal.signal(signal.SIGTERM, self._signal_cb)
        signal.signal(signal.SIGINT, self._signal_cb)

    def _signal_cb(self, signum: int, *args: Any) -> None:
        signame = {2: 'INT', 15: 'TERM'}.get(signum, signum)
        logger.warning('AgentsManager: catch %s', signame)
        self.running = False

    def start(self) -> None:
        ''' Starting and keep running unix-processes with agents '''

        self.running = True
        logger.info('AgentsManager: starting...')

        for name, _cfg in self.cfg:
            proc = self.mp_ctx.Process(target=run_agent, name=name, args=(name, *_cfg), daemon=True)
            proc.start()

            if not proc.pid:
                logger.error('AgentsManager: fail starting %s', name)
                self.close()
                return

            self.processes[proc.pid] = proc
            logger.info('AgentsManager: %s started', name)

        logger.info('AgentsManager: started\n%s', self._get_state())

        while self.running:
            time.sleep(.01)

            if any(not p.is_alive() for p in self.processes.values()):
                self.running = False  # one finished - all finished

        logger.info('AgentsManager: interrupt detected')
        self.close()

    def close(self) -> None:
        ''' Terminating dependent processes '''

        logger.warning('AgentsManager: force kill processes\n%s', self._get_state())

        for process in self.processes.values():
            logger.info('AgentsManager: terminate %s [%s]', process.name, process.pid)
            process.terminate()

        time.sleep(.5)
        logger.info('AgentsManager: forked processes killed\n%s', self._get_state())

    def _get_state(self) -> str:
        result = []

        for pid, proc in self.processes.items():
            _color = 32 if proc.is_alive() else 31
            _state = 'running...' if proc.is_alive() else f'exit={proc.exitcode}'
            result.append(f'  {pid}: {proc.name} \x1b[{_color}m[{_state}]\x1b[0m')

        return '\n'.join(result)


def run() -> None:
    ''' Parse input and run '''

    parser = argparse.ArgumentParser(description='Run microagents')
    parser.add_argument('modules', metavar='MODULE_PATH', type=str, nargs='+',
        help='configuration modeles path')

    call_args = parser.parse_args()
    cfg = list(chain(*[load_configuration(module) for module in call_args.modules]))

    try:
        AgentsManager(cfg).start()
    finally:
        parser.exit(message='Exit\n')
