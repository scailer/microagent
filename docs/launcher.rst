Launcher and configuration
======================================

Configuration file
------------------

Configuration JSON file supports optional ``default_prefix`` field to set
the global channel prefix for all buses and brokers.

.. include:: ../examples/signals.json
   :code: json


Configuration Python file
-------------------------

.. automodule:: microagent.launcher


.. include:: ../examples/settings.py
   :code: python


.. autoclass:: microagent.launcher.ServerInterrupt


.. autofunction:: microagent.launcher.load_configuration


.. autofunction:: microagent.launcher.init_agent


.. autoclass:: microagent.launcher.AgentsManager


.. autoclass:: microagent.launcher.GroupInterrupt
