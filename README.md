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
$ python3 setup.py develop
$ pip install -r examples/requirements.txt
```

make run_pulsar_example
make run_aioredis_example

