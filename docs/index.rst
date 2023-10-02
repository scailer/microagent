.. MicroAgent documentation master file, created by
   sphinx-quickstart on Sat Mar  2 11:28:52 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

MicroAgent documentation
======================================

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
* specification of signals (events), their sending and receiving via the bus (:ref:`redis <bus>`)
* description of queues, sending and receiving messages via the queue broker (amqp_, kafka_, redis_)
* limited **RPC** via signal bus
* launching sub-services (in the same process)
* launching a group of microagents (each in a separate process)
* mocks for bus and broker


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


.. toctree::
   :maxdepth: 1
   :caption: Core:

   agent
   periodic
   bus
   broker
   hooks
   launcher

.. toctree::
   :maxdepth: 1
   :caption: Tools:

   redis
   amqp
   kafka
   mocks

.. toctree::
   :maxdepth: 1
   :caption: Examples:

   example_agents
   example_run


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _redis: https://redis.readthedocs.io
.. _kafka: kafka.html
.. _amqp: amqp.html
