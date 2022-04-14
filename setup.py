import re
import os
import pathlib
from setuptools import setup  # type: ignore


try:
    version = re.findall(r"^__version__ = '([^']+)'\r?$",
        (pathlib.Path(__file__).parent / 'microagent' / '__init__.py').read_text('utf-8'), re.M)[0]
except IndexError:
    raise RuntimeError('Unable to determine version.')


def read(filename):
    with open(os.path.join(filename), 'rt') as f:
        return f.read().strip()


setup(
    version=version,
    packages=['microagent', 'microagent.tools'],
    long_description=read('README.rst'),
    include_package_data=True,
    install_requires=[],

    package_data={
        'microagent': ['py.typed'],
    },

    setup_requires=["pytest-runner"],

    tests_require=[
        'ujson',
        'pytest',
        'pytest-asyncio',
        'pytest-mypy',
        'asynctest',
        'pytest-cov',
        'aioredis==2.0',
        'aioamqp==0.14',
        'aiokafka==0.7',
        'pytest-flake8',
        'flake8-print',
        'flake8-blind-except==0.1.1',
        'flake8-builtins==1.4.1',
    ],

    extras_require={
        'aioredis': ['aioredis==2.0'],
        'amqp': ['aioamqp==0.15'],
        'kafka': ['aiokafka==0.7'],
    },

    entry_points={
        'console_scripts': [
            'marun = microagent.launcher:run',
        ]
    }
)
