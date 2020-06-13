import asyncio
import logging
import sys
import os

import aioredis

from microagent import MicroAgent, periodic, receiver, consumer, load_stuff, cron, on
from microagent.tools.aioredis import AIORedisSignalBus, AIORedisBroker


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
cur_dir = os.path.dirname(os.path.realpath(__file__))
signals, queues = load_stuff('file://' + os.path.join(cur_dir, 'signals.json'))
#####################################################################################################

class PostAgent(MicroAgent):
    counter = 0

    @receiver(signals.post)
    async def post_handler(self, post, user, **kw):
        self.counter += 1
        self.log.info('Catch post: %d %s', self.counter, post)
        # Отправляем данные поста в очередь 'post'
        await self.broker.post.send(
            {'post': post, 'author': user, 'num': self.counter})
        await self.bus.new_post.send('post_agent', num=self.counter)


class UserAgent(MicroAgent):
    @on('pre_start')  # Вызывается перед подключение обработчиков к шине / брокеру
    async def setup(self):
        self.redis = await aioredis.create_redis('redis://localhost:6379/7')
        self.users = set(await self.redis.smembers('users'))

    @on('post_stop')  # Вызывается после отключения обработчиков от шины / брокера
    async def shutdown(self):
        self.redis.close()
        await self.redis.wait_closed()

    @periodic(period=1, timeout=1)  # Раз в секунду проверям данные в редисе, timeout - 1 сек
    async def users_watcher(self):
        users = set(await self.redis.smembers('users'))

        for user in users - self.users:
            await self.bus.user_enter.send('user_agent', name=user)  # Отправляем сигнал 'user_enter'
            self.log.info('User enter: %s', user)

        for user in self.users - users:
            await self.bus.user_exit.send('user_agent', name=user)  # Отправляем сигнал 'user_exit'
            self.log.info('User exit: %s', user)

        self.users = users

    @on('error_users_watcher')  # Вызывается при исключении в методе users_watcher
    async def err_sender(self, *args, **kwargs):
        self.log.error('Error %s %s', args, kwargs)

    @receiver(signals.check_user)  # Принимаем сигнал 'check_user' и отправляем ответ
    async def check_user(self, user, **kw):
        return user in self.users


class MentionAgent(MicroAgent):
    @on('pre_start')
    async def setup(self):
        self.users = set()

    @receiver(signals.user_enter, signals.user_exit, timeout=10)  # Подписываемся сразу на 2 сигнала
    async def update_users(self, signal, name, **kw):
        self.log.info('Update users: %s', name)

        if signal == signals.user_enter:
            self.users.add(name)

        if signal == signals.user_exit:
            self.users.remove(name)

    # Принимаем данные из очереди 'post'
    # autoack - автоматически удаляем/помечаем обработанным пакет из очереди
    @consumer(queues.post, autoack=True)
    async def main_handler(self, *args, **data):
        if data['author'] not in self.users:
            # Отправляем сигнал 'check_user' и ожидаем первого ответа
            resp = await self.bus.check_user.call('mention_agent', user=data['author'])

            if resp:
                self.users.add(data['author'])
            else:
                raise Exception('Unknown user')

        for name in self.users:
            if f'@{name}' in data['post']:
                # Отправляем данные уведомления в очередь 'email'
                await self.broker.email.send({'user': name, 'post': data['post']})


class StatsAgent(MicroAgent):
    posts_qty = [0, 0]

    @receiver(signals.new_post)
    async def collect_stats(self, num, **kw):
        self.posts_qty[0] = num

    @cron('0 */4 * * *', timeout=30)  # Раз в 4 часа создаем отчет с собранной статистикой
    async def send_report(self):
        text = 'Get %d new posts by 4 hours' % (self.posts_qty[0] - self.posts_qty[1])
        self.posts_qty[1] = self.posts_qty[0]


async def _main():
    bus = AIORedisSignalBus('redis://localhost/7')
    broker = AIORedisBroker('redis://localhost/7')
    await UserAgent(bus).start()
    await PostAgent(bus, broker=broker).start()
    await MentionAgent(bus, broker=broker).start()
    await StatsAgent(bus).start()


def main():
    asyncio.ensure_future(_main())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.call_soon(main)
    loop.run_forever()
