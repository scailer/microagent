import os
import signal
import asyncio
import argparse
import importlib
import logging
import concurrent.futures
from itertools import chain
from functools import partial
from multiprocessing import parent_process


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


def load_configuration(config_path: str):
    '''
        Load configuration from module and prepare it for initializint agents
    '''

    mod = importlib.import_module(config_path)
    _agents = getattr(mod, 'AGENT', {})
    _brokers = getattr(mod, 'BROKER', {})
    _buses = getattr(mod, 'BUS', {})

    for name, cfg in _agents.items():
        cfg['bus'] = _buses.get(cfg.pop('bus', None))
        cfg['broker'] = _brokers.get(cfg.pop('broker', None))
        yield name, cfg


def init_agent(cfg: dict):
    '''
        Initialize agent from config-dict
    '''

    bus = cfg.pop('bus', None)
    broker = cfg.pop('broker', None)

    if bus:
        Bus = _import(bus.pop('backend'))
        bus = Bus(**bus)

    if broker:
        Broker = _import(broker.pop('backend'))
        broker = Broker(**broker)

    Agent = _import(cfg.pop('backend'))
    return Agent(bus=bus, broker=broker, **cfg)


def _import(path):
    mod = importlib.import_module('.'.join(path.split('.')[:-1]))
    return getattr(mod, path.split('.')[-1])


def _run_agent(name, cfg):
    '''
        Initialize and run microagent
    '''
    logger.info('Run Agent %s pid#%s', name, os.getpid())

    async def _run():
        loop = asyncio.get_event_loop()

        # Interrupt process when master shutdown
        loop.add_signal_handler(signal.SIGINT, partial(_interrupter, 'INT'))
        loop.add_signal_handler(signal.SIGTERM, partial(_interrupter, 'TERM'))

        # Check master & force break
        loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, parent_process().pid, loop)

        agent = init_agent(cfg)

        try:
            await agent.start()  # wait when servers used
        except (KeyboardInterrupt, GroupInterrupt):
            loop.stop()
            return
        except Exception as exc:
            logger.error('Catch error %s', exc, exc_info=True)
            raise

        while True:  # wait when no servers in agent
            logger.debug('Agent %s alive', name)
            await asyncio.sleep(3600)

    asyncio.run(_run())


def _interrupter(signal):
    logger.warning('Catch %s signal', signal)
    raise GroupInterrupt(signal)


def _master_watcher(pid, loop):
    loop.call_later(MASTER_WATCHER_PERIOD, _master_watcher, pid, loop)
    try:
        os.kill(pid, 0)  # check master process
    except ProcessLookupError:
        os._exit(os.EX_OK)  # hard break better than deattached pocesses


async def _run_master(cfg):
    with concurrent.futures.ProcessPoolExecutor(len(cfg)) as pool:
        pool.interrupter_lock = False
        signal.signal(signal.SIGTERM, partial(_signal_cb, pool=pool))
        signal.signal(signal.SIGINT, partial(_signal_cb, pool=pool))
        futures = []

        for name, _cfg in cfg:
            fut = pool.submit(_run_agent, name, _cfg)  # run agent in forked process
            fut.add_done_callback(partial(_stop_cb, name))  # subscribe for finishing
            futures.append(fut)

        try:
            # In normal way waiting forevevr
            data = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

            # If one of agents stoped, close all agents (one fail, all fail)
            if data.not_done:
                _close_pool(pool)

        except KeyboardInterrupt:
            logger.waiting('Quit')

        except Exception as exc:
            logger.error('Quit with error %s', exc, exc_info=True)
            raise

    logger.info('Agents stoped')


def _stop_cb(name, future):
    logger.info('Agent %s stoped with %s', name, future)


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
