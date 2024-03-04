.. _bus:

Signal bus
======================================


.. automodule:: microagent.bus


.. autofunction:: microagent.load_signals


.. autofunction:: microagent.receiver


.. autoclass:: microagent.bus.AbstractSignalBus
    :members:
    :member-order: bysource


.. autoclass:: Signal
    :members:
    :member-order: bysource


**Internals stuff for signal bus binding**

.. autoclass:: microagent.bus.BoundSignal
    :members:


.. autoclass:: microagent.bus.Receiver


**Exceptions**

.. autoclass:: microagent.signal.SignalException


.. autoclass:: microagent.signal.SignalNotFound


.. autoclass:: microagent.signal.SerializingError
