MicroAgent
==========

.. image:: https://img.shields.io/pypi/v/microagent.svg
   :target: https://pypi.python.org/pypi/microagent

.. image:: https://img.shields.io/pypi/pyversions/microagent.svg
  :target: https://pypi.python.org/pypi/microagent

.. image:: https://img.shields.io/pypi/l/microagent.svg
  :target: https://pypi.python.org/pypi/microagent

.. image:: https://img.shields.io/pypi/status/microagent.svg
  :target: https://pypi.python.org/pypi/microagent

.. image:: https://img.shields.io/pypi/dd/microagent.svg
  :target: https://pypi.python.org/pypi/microagent

.. image:: https://codecov.io/gh/scailer/microagent/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/scailer/microagent

.. image:: https://travis-ci.com/scailer/microagent.svg?branch=master
  :target: https://travis-ci.con/scailer/microagent

.. image:: https://readthedocs.org/projects/microagent/badge/?version=latest&style=flat
  :target: https://microagent.readthedocs.io/


The goal of this project is to facilitate the creation of **microservices**
interacting via a **signal bus** and/or **queue broker**.

The philosophy of this project is to present a microservice as a software agent
that directly interacts only with queues and the event bus, and not with other microservices.

Tool is intended for developing:

* distributed apps with **event-driven** architecture
* distributed apps with **data-driven** architecture
* multi-processors apps 


Tool provide features:

* running a **periodical tasks** (interval or as CRON)
* specification of signals (events), their sending and receiving via the bus (redis_)
* description of queues, sending and receiving messages via the queue broker (aioamqp_, kafka_, redis_)
* limited **RPC** via signal bus
* launching sub-services (in the same process)
* launching a group of microagents (each in a separate process)
* mocks for bus and broker


See MicroAgent documentation_.


.. code-block:: python

    class Agent(MicroAgent):

        @on('pre_start')
        async def setup(self):
            pass  # init connections to DB, REDIS, SMTP and other services

        @periodic(period=5)
        async def refresh_cache(self):
            pass  # do something periodicly

        @cron('0 */4 * * *')
        async def send_report(self):
            pass  # do something by schedule

        # subscribe to signals (events)
        @receiver(signals.user_updated, signals.user_deleted)
        async def send_notification(self, **kwargs):
            # send signal (event) to bus
            await self.bus.check_something.send(sender='agent', **kwargs)

        # message consumer from queue
        @consumer(queues.mailer)
        async def send_emails(self, **kwargs):
            # send message to queue
            await self.broker.statistic_collector.send(kwargs)


    async def main():
        bus = RedisSignalBus('redis://localhost/7')
        broker = RedisBroker('redis://localhost/7')

        # usage bus and broker separate from agent
        await bus.started.send('user_agent')
        await broker.mailer(data)

        agent = Agent(bus=bus, broker=broker)
        await agent.start()

Installing
----------

With redis_ backend provide signal bus and list-based queues::

    pip install 'microagent[redis]'

With aioamqp_ backend provide queues over AMQP (RabbitMQ)::

    pip install 'microagent[amqp]'

With kafka_ backend provide queues over Kafka (experemental)::

    pip install 'microagent[kafka]'


.. _redis: https://pypi.org/project/redis/
.. _aioamqp: https://pypi.org/project/aioamqp/
.. _kafka: https://pypi.org/project/aiokafka/
.. _documentation: https://microagent.readthedocs.io/
