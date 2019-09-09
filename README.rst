MicroAgent
==========

MicroAgent is a tool for building multiagent ecosystems for distributed programs. MicroAgent allow you faster write apps on event-oriented and data-driven architectures. It provide universal API for work with event bus and queue brokers, and periodical tasks - by interval and "as cron".

Installing
----------

With aioredis_ backend provide signal bus and list-based queues::

    pip install 'microagent[aioredis]'

With aioamqp_ backend provide queues over AMQP (RabbitMQ)::

    pip install 'microagent[amqp]'

With kafka_ backend provide queues over Kafka (experemental)::

    pip install 'microagent[kafka]'

With pulsar_ backend provide signal bus (Redis) and list-based queues (Redis)::

    pip install 'microagent[pulsar]'

With mocks for writing tests::

    pip install 'microagent[mock]'

Start
-----

Minimal working example

.. code-block:: python

    import asyncio
    from microagent import MicroAgent, periodic

    class MyAgent(MicroAgent):
        @periodic(period=1, timeout=1)
        async def hello(self):
            print('Hellow world!')

    async def main():
        await MyAgent().start()

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.call_soon(lambda: asyncio.ensure_future(main()))
        loop.run_forever()


Event-driven
------------

.. code-block:: python

    import os
    import asyncio

    from microagent import MicroAgent, periodic, receiver, load_stuff
    from microagent.tools.aioredis import AIORedisSignalBus

    cur_dir = os.path.dirname(os.path.realpath(__file__))
    signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'events.json'))


    class SenderAgent(MicroAgent):
        @periodic(period=1, timeout=1)
        async def sender(self):
            await self.bus.my_event.send('sender_name', val1=1, val2='a')


    class CatcherAgent(MicroAgent):
        @receiver(signals.my_event)
        async def catcher(self, signal, sender, val1, val2):
            print(f'Catch {signal} {sender} {val1} {val2}')


    async def main():
        bus = AIORedisSignalBus('redis://localhost/7')
        await SenderAgent(bus=bus).start()
        await CatcherAgent(bus=bus).start()

events.json

.. code-block:: javascript

    {
        "version": 1,
        "signals": [
            {"name": "my_event", "providing_args": ["val1", "val2"]}
        ]
    }
    
Run

.. code-block::

    $ python3 examples/event_driven.py
    Catch <Signal my_event> sender_name 1 a
    Catch <Signal my_event> sender_name 1 a
    Catch <Signal my_event> sender_name 1 a


Data-driven
-----------

.. code-block:: python

    import os
    import asyncio

    from microagent import MicroAgent, periodic, consumer, load_stuff
    from microagent.tools.aioredis import AIORedisBroker

    cur_dir = os.path.dirname(os.path.realpath(__file__))
    signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'queues.json'))


    class SenderAgent(MicroAgent):
        @periodic(period=1, timeout=1)
        async def sender(self):
            await self.broker.my_queue.send({'val1': 1, 'val2': 'a'})


    class CatcherAgent(MicroAgent):
        @consumer(queues.my_queue)
        async def catcher(self, val1, **data):
            print(f'Catch {val1} {data}')


    async def main():
        broker = AIORedisBroker('redis://localhost/7')
        await SenderAgent(broker=broker).start()
        await CatcherAgent(broker=broker).start()

queues.json

.. code-block:: javascript

    {
        "version": 1,
        "queues": [
            {"name": "my_queue"}
        ]
    }
    
Run

.. code-block::

    $ python3 examples/data_driven.py
    Catch 1 {'val2': 'a'}
    Catch 1 {'val2': 'a'}
    Catch 1 {'val2': 'a'}


Sync handling with RPC
----------------------

.. code-block:: python

    import os
    import sys
    import time
    import asyncio
    import logging

    from microagent import MicroAgent, periodic, receiver, load_stuff
    from microagent.tools.aioredis import AIORedisSignalBus

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'events.json'))


    class SenderAgent(MicroAgent):
        @periodic(period=5, timeout=5)
        async def sender(self):
            self.log.info('Begin at %s', time.asctime())
            response = await self.bus.my_event.call('sender_name', val1=1, val2='a')
            self.log.info('Finish at %s with %s', time.asctime(), response)


    class CatcherAgent(MicroAgent):
        @receiver(signals.my_event)
        async def catcher(self, signal, sender, val1, val2):
            self.log.info(f'Catch {signal} {sender} {val1} {val2}')
            await asyncio.sleep(3)
            return 1


    async def main():
        bus = AIORedisSignalBus('redis://localhost/7')
        await SenderAgent(bus=bus).start()
        await CatcherAgent(bus=bus).start()

Run

.. code-block::

    $ python3 examples/remote_call.py
    INFO:microagent:Begin at Mon Sep  8 09:32:44 2019
    INFO:microagent:Catch <Signal my_event> sender_name 1 a
    INFO:microagent:Finish at Mon Sep  8 09:32:47 2019 with {'CatcherAgent.catcher': 1}


Periodic tasks
--------------

.. code-block:: python

    import asyncio
    from microagent import MicroAgent, periodic, cron

    class MyAgent(MicroAgent):
        @periodic(period=1, timeout=1, start_after=5)
        async def hello(self):
            print('Hellow world!')

        @cron('*/2 * * * *', timeout=10)
        async def two_min(self):
            print('Run every 2 min')

    async def main():
        await MyAgent().start()

.. code-block::

    $ python3 examples/periodic.py
    Hellow world!
    Hellow world!
    Run every 2 min
    Hellow world!

Hooks
-----

.. code-block:: python

    from microagent import MicroAgent, on

    class MyAgent(MicroAgent):
        @on('pre_start')
        async def call_first(self):
            print('Call before recivers and consumers will be enabled')

        @on('post_start')
        async def call_second(self):
            print('Call after recivers and consumers will be enabled')
            
        @on('pre_stop')
        async def call_last(self):
            print('Call when agent.stop() called')
        
    async def run():
        agent = MyAgent()
        await agent.start()
        print('Working...')
        await agent.stop()


    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
        loop.close()


.. code-block::

    $ python3 examples/hooks.py
    Call before recivers and consumers will be enabled
    Call after recivers and consumers will be enabled
    Workng...
    Call when agent.stop() called


.. _aioredis: https://pypi.org/project/aioredis/
.. _aioamqp: https://pypi.org/project/aioamqp/
.. _kafka: https://pypi.org/project/aiokafka/
.. _pulsar: https://pypi.org/project/pulsar/
