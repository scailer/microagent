from setuptools import setup


setup(
    packages=['microagent'],
    include_package_data=True,
    install_requires=[
        'ujson',
        'requests',
        'croniter'
    ],

    setup_requires=["pytest-runner"],
    tests_require=[
        'pytest',
        'pytest-pep8',
        'pudb',
        'pytest-pudb',
        'asynctest',
        'pytest-cov',
        'pulsar==1.6.4',
        'aioredis',
        'aioamqp==0.12.0'
    ],

    extras_require={
        'pulsar': ['pulsar==1.6.4'],
        'aioredis': ['aioredis'],
        'amqp': ['aioamqp==0.12.0'],
    },
)
