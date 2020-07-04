'''
Configuration and launch MicroAgents with shipped launcher.
Configuration file is a python-file with 3 dictionaries:
AGENT, BUS and BROKER, where specified all settings.
Launcher can run microagents from one or several files.

.. code-block::  shell

    $ marun myproject.app1 myproject.app2


Each microagent is launched in a separate system thread, and the launcher works
as a supervisor. All agents started by a single command are called a deployment
group and start/stop at the same time. If one of the agents stops, the launcher
stops the entire group.
'''

import os
import signal
import asyncio
import argparse
import importlib
import logging
import concurrent.futures
from typing import Optional, Iterator, Tuple, Dict, List, Any, TYPE_CHECKING
from itertools import chain
from functools import partial
from multiprocessing import parent_process

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

    bus, broker = None, None
    _bus: Optional[CFG_T] = cfg.pop('bus', None)
    _broker: Optional[CFG_T] = cfg.pop('broker', None)

    if _bus:
        bus: 'AbstractSignalBus' = _import(_bus[0])(**_bus[1])

    if _broker:
        broker: 'AbstractQueueBroker' = _import(_broker[0])(**_broker[1])

    return _import(backend)(bus=bus, broker=broker, **cfg)


def _import(path: str):
    mod = importlib.import_module('.'.join(path.split('.')[:-1]))
    return getattr(mod, path.split('.')[-1])


def _run_agent(name: str, backend: str, cfg: Dict[str, Any]) -> None:
    '''
        Initialize MicroAgent instance from config and run it forever
        Contains handling for process control
    '''
    logger.info('Run Agent %s pid#%s', name, os.getpid())
    asyncio.run(run_agent(name, backend, cfg, parent_process().pid))


async def run_agent(name: str, backend: str, cfg: Dict[str, Any], pid: int = None) -> None:
    if pid:  # pid if run in coprocess
        loop = asyncio.get_event_loop()

        # Interrupt process when master shutdown
        loop.add_signal_handler(signal.SIGINT, partial(_interrupter, 'INT'))
        loop.add_signal_handler(signal.SIGTERM, partial(_interrupter, 'TERM'))

        # Check master & force break
        loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, pid, loop)

    agent = init_agent(backend, cfg)

    try:
        await agent.start()  # wait when servers used

    except (KeyboardInterrupt, GroupInterrupt):
        if pid:
            loop = asyncio.get_event_loop()
            loop.stop()
        return

    except Exception as exc:
        logger.error('Catch error %s', exc, exc_info=True)
        raise

    while True:  # wait when no servers in agent
        logger.debug('Agent %s alive', name)
        await asyncio.sleep(3600)


def _interrupter(sig):
    logger.warning('Catch %s signal', sig)
    raise GroupInterrupt(sig)


def _master_watcher(pid: int, loop: asyncio.BaseEventLoop):
    loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, pid, loop)
    try:
        os.kill(pid, 0)  # check master process
    except ProcessLookupError:
        os._exit(os.EX_OK)  # noqa: W0212 hard break better than deattached pocesses


async def _run_master(cfg: List[Tuple[str, CFG_T]]):
    with concurrent.futures.ProcessPoolExecutor(len(cfg)) as pool:
        pool.interrupter_lock = False
        signal.signal(signal.SIGTERM, partial(_signal_cb, pool=pool))
        signal.signal(signal.SIGINT, partial(_signal_cb, pool=pool))
        futures = []

        for name, _cfg in cfg:
            fut = pool.submit(_run_agent, name, *_cfg)  # run agent in forked process
            fut.add_done_callback(partial(_stop_cb, name))  # subscribe for finishing
            futures.append(fut)

        try:
            # In normal way waiting forevevr
            data = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

            # If one of agents stoped, close all agents (one fail, all fail)
            if data.not_done:
                _close_pool(pool)

        except KeyboardInterrupt:
            logger.warning('Quit')

        except Exception as exc:
            logger.error('Quit with error %s', exc, exc_info=True)
            raise

    logger.info('Agents stoped')


def _stop_cb(name, future):
    logger.info('Agent %s stoped with %s', name, future)
    try:
        future.result()
    except Exception as exc:
        print(exc)  # noqa: T001


def _signal_cb(signum, *args, pool):
    signame = {2: 'INT', 15: 'TERM'}.get(signum, signum)
    logger.warning('Catch %s %s', signame, list(pool._processes))
    _close_pool(pool)


def _close_pool(pool):
    if pool.interrupter_lock:  # Prevent duble kill
        logger.warning('Force kill locked')
        return

    pool.interrupter_lock = True
    logger.warning('Force kill processes %s', list(pool._processes))

    for pid in pool._processes:
        try:
            os.kill(pid, signal.SIGINT)
        except ProcessLookupError:
            logger.info('Process %s already killed', pid)

    logger.info('Forked processes killed')


async def main():
    ''' Parse input and run '''

    parser = argparse.ArgumentParser(description='Run microagents')
    parser.add_argument('modules', metavar='MODULE_PATH', type=str, nargs='+',
        help='configuration modeles path')
    # parser.add_argument('--bind', metavar='HOST:POST', type=str, dest='bind',
    #     help='Bind duty interface')

    call_args = parser.parse_args()
    cfg = list(chain(*[load_configuration(module) for module in call_args.modules]))

    try:
        await _run_master(cfg)
    finally:
        parser.exit(message="Exit\n")


def run():
    asyncio.run(main())
