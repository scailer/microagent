|pypiversion|  |pyversions|  |license|  |status|  |downloads|  |travis|  |coverage|  |docs|


.. |pypiversion| image:: https://img.shields.io/pypi/v/microagent.svg
    :target: https://pypi.python.org/pypi/microagent
.. |pyversions| image:: https://img.shields.io/pypi/pyversions/microagent.svg
  :target: https://pypi.python.org/pypi/microagent
.. |license| image:: https://img.shields.io/pypi/l/microagent.svg
  :target: https://pypi.python.org/pypi/microagent
.. |status| image:: https://img.shields.io/pypi/status/microagent.svg
  :target: https://pypi.python.org/pypi/microagent
.. |downloads| image:: https://img.shields.io/pypi/dd/microagent.svg
  :target: https://pypi.python.org/pypi/microagent
.. |contributors| image:: https://img.shields.io/github/contributors/scailer/microagent.svg
  :target: https://github.com/scailer/microagent/graphs/contributors
.. |coverage| image:: https://codecov.io/gh/scailer/microagent/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/scailer/microagent
.. |travis| image:: https://api.travis-ci.org/scailer/microagent.svg?branch=master
  :target: https://travis-ci.org/scailer/microagent
.. |docs| image:: https://readthedocs.org/projects/microagent/badge/?version=latest&style=flat
  :target: https://microagent.readthedocs.io/


## Usage ##

Пишем агент с периодической функцией и приемником сигналов

```python
from microagent import MicroAgent, receiver, periodic

class MyAgent(MicroAgent):
    def setup(self):
        redis_store = create_store(
            self.settings.get('redis_server').value,
            decode_responses=True, loop=self._loop)
        self.redis = redis_store.client()

        self.pg = create_store(
            self.settings.get('pg_server').value,
            loop=self._loop, pool_size=3, timeout=60.0)

    @periodic(period=60 * 60, timeout=30 * 60, start_after=60 * 60)
    async def check_reply(self):
        # эта функция запуститься сама через час и будет запускаться раз в час
        pass

    @receiver([signals.post_send, signals.post_edit])
    async def post_notification(self, author_id, thread_id, **kwargs):
        # будет вызываться по сигналам
        pass
```

Подключаем наш агент в сервис

```python
from microagent.tools.pulsar import MicroAgentApp

APPS = {
    'lrp': {
        '': (MicroAgentApp, ),
        'agent': MyAgent,
    },
    ...
}
```


## Example ##

```bash
$ mkvirtualenv -p python3.6 microagent
$ pip install -e .
$ pip install -r examples/requirements.txt
```

make run_pulsar_example
make run_aioredis_example

