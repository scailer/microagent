.. _bus:

Signal bus
======================================


Default channel prefix
----------------------

Channel prefix is used to construct channel names for signals on the bus.
It can be set in two ways:

1. Explicitly via ``prefix`` argument to bus constructor
2. Globally via ``default_prefix`` field in configuration JSON file

If ``default_prefix`` is specified in the config file, it overrides the
default value (``PUBSUB``) for all signal buses created after
``configure()`` is called. Explicit ``prefix`` argument always overrides
the global default.

.. code-block:: python

    from microagent import configure
    from microagent.tools.redis import RedisSignalBus

    # signals.json: {"default_prefix": "MYAPP", "signals": [...]}
    configure('file://signals.json')

    # Bus will use 'MYAPP' as prefix (from default_prefix in json)
    bus = RedisSignalBus('redis://localhost/7')

    # Explicit prefix overrides the global default
    bus2 = RedisSignalBus('redis://localhost/7', prefix='OTHER')


.. automodule:: microagent.bus


.. autofunction:: microagent.configure


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


.. autoclass:: microagent.DoubleLoadError
