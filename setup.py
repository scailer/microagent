from setuptools import setup


setup(
    name='microagent',
    version='0.1.0',
    author='Dmitriy Vlasov',
    author_email='scailer@yandex.ru',

    packages=['microagent'],
    include_package_data=True,
    install_requires=[
        'ujson',
        'requests',
    ],

    setup_requires=["pytest-runner"],
    tests_require=['pytest'],

    extras_require={
        'pulsar': ['pulsar==1.6.4'],
        'aioredis': ['aioredis'],
    },

    url='https://gitlab.com/tamtam-im/pulsar-microserver',
    license='MIT license',
    description='Multiagent framework',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
