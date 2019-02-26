from setuptools import setup


setup(
    packages=['microagent', 'microagent.tools'],
    include_package_data=True,
    install_requires=[
        'ujson',
        'requests',
        'croniter'
    ],

    setup_requires=["pytest-runner"],
    tests_require=[
        'pytest',
        'pytest-asyncio',
        'asynctest',
        'pytest-cov',
        'pulsar==2.0.2',
        'aioredis',
        'aioamqp==0.12.0',
        'pytest-flake8'
    ],

    extras_require={
        'pulsar': ['pulsar'],
        'aioredis': ['aioredis'],
        'amqp': ['aioamqp==0.12.0'],
        'mock': ['asynctest'],
    },
)
