Configuration, launch and etc.
======================================


Configuration file (signals.json)
---------------------------------

Configuration JSON file supports optional ``default_prefix`` field to set
the global channel prefix for all buses and brokers.

.. include:: ../examples/signals.json
   :code: json


Configuration Python file (settings.py)
----------------------------------------

.. include:: ../examples/settings.py
   :code: python

Run in shell:

    $ marun examples.settings


Custom server setup and starting (redis_server.py)

.. include:: ../examples/redis_server.py
   :code: python


Run in shell:

    $ python examples/redis_server.py
