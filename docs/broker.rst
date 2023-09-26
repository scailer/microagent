.. _broker:

Queue broker
======================================


.. automodule:: microagent.broker


.. autofunction:: microagent.load_queues


.. autofunction:: microagent.consumer


.. autoclass:: microagent.broker.AbstractQueueBroker
    :members:
    :member-order: bysource


.. autoclass:: microagent.queue.Queue
    :members:
    :member-order: bysource


**Internals stuff for queues broker binding**

.. autoclass:: microagent.broker.BoundQueue
    :members:


.. autoclass:: microagent.broker.Consumer



**Exceptions**

.. autoclass:: microagent.queue.QueueException


.. autoclass:: microagent.queue.QueueNotFound


.. autoclass:: microagent.queue.SerializingError
