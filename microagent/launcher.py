import os
import asyncio
import argparse
import importlib
import logging
import concurrent.futures
from itertools import chain


logger = logging.getLogger('microagent.run')

parser = argparse.ArgumentParser(description='Run microagents')
parser.add_argument('modules', metavar='MODULE_PATH', type=str, nargs='+',
    help='configuration modeles path')
# parser.add_argument('--bind', metavar='HOST:POST', type=str, dest='bind',
#     help='Bind duty interface')


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
        agent = init_agent(cfg)
        await agent.start()

        while True:
            logger.debug('Agent %s alive', name)
            await asyncio.sleep(3600)

    asyncio.run(_run())


async def main():
    call_args = parser.parse_args()
    _cfg = list(chain(*[load_configuration(module) for module in call_args.modules]))

    with concurrent.futures.ProcessPoolExecutor(len(_cfg)) as pool:
        futures = [pool.submit(_run_agent, name, cfg) for name, cfg in _cfg]

        try:
            [fut.result() for fut in concurrent.futures.as_completed(futures)]
        except (KeyboardInterrupt, concurrent.futures.process.BrokenProcessPool):
            logger.error('Agent shoutdown', exc_info=True)
        except Exception as exc:
            logger.error('Agent error %s', exc, exc_info=True)
            raise

    logger.info('Agents stoped')


def run():
    asyncio.run(main())
