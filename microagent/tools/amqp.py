'''
:ref:`Queue Broker <broker>` based on :aioamqp:`aioamqp <>`.
'''
import asyncio
import logging
import time

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from aiormq import Connection
from aiormq.abc import AbstractChannel, AbstractConnection, Basic, DeliveredMessage, ExceptionType
from aiormq.exceptions import AMQPError, ConnectionClosed

from ..broker import AbstractQueueBroker, Consumer


log = logging.getLogger('microagent.amqp')
ConnectionClosedDefault = ConnectionClosed(0, 'normal closed')
AMQPWrapper = Callable
REBIND_ATTEMPTS = 3
REBIND_BASE_DELAY = 10


@dataclass
class AMQPBroker(AbstractQueueBroker):
    '''
        The broker is based on the basic_consume method of the AMQP and sends
        a acknowledgement automatically if the handler is completed without errors.
        The consumer takes an exclusive channel. Sending an reuse the channels.

        :param dsn: string, data source name for connection amqp://guest@localhost:5672/
        :param log: logging.Logger (optional)

        .. code-block:: python

            from microagent.tools.amqp import AMQPBroker

            broker = AMQPBroker('amqp://guest:guest@localhost:5672/')

            await broker.user_created.send({'user_id': 1})


        `@consumer`-decorator for this broker has an additional option - `autoack`,
        which enables / disables sending automatic acknowledgements.

        .. code-block:: python

            class EmailAgent(MicroAgent):
                @consumer(queues.mailer, autoack=False)
                async def example_read_queue(self, amqp, **data):
                    await amqp.channel.basic_client_ack(delivery_tag=amqp.delivery_tag)


        Handler will takes one required positional argument - :ref:`pamqp.DeliveredMessage`.
        Consumer will be reconnect and subscribe to queue on disconnect.
        It make 3 attempts of reconnect after 1, 4, 9 seconds.
        if the queue does not exist, it will be declared with the default parameters when binding.

    '''
    connection: AbstractConnection = field(init=False)
    sending_channel: AbstractChannel | None = None

    def __post_init__(self) -> None:
        self.connection = ReConnection(self.reconnect, self.dsn)

    async def reconnect(self) -> None:
        self.connection = ReConnection(self.reconnect, self.dsn)
        await self.connection.connect()
        log.info('Reconnect "%s"', self.connection)

    async def get_channel(self) -> AbstractChannel:
        '''
            Takes a channel from the pool or a new one, performs a lazy connection if required.
        '''
        if not self.connection.is_opened:
            await self.reconnect()

        if not self.sending_channel or self.sending_channel.is_closed:
            self.sending_channel = await self.connection.channel()

        return self.sending_channel

    async def send(self, name: str, message: str, exchange: str = '',
            properties: dict | None = None, **kwargs: Any) -> None:
        '''
            Raw message sending.

            :param name: string, target queue name (routing_key)
            :param message: string, serialized message
            :param exchange: string, target exchange name
            :param properties: dict, for Basic.Properties
            :param \*\*kwargs: dict, other basic_publish options
        '''  # noqa: W605

        channel = await self.get_channel()
        await channel.basic_publish(message.encode(), routing_key=name, exchange=exchange,
            properties=Basic.Properties(**properties) if properties else None, **kwargs)

    async def bind(self, name: str) -> None:
        await ManagedConnection(
            dsn=self.dsn,
            queue_name=name,
            handler=self._amqp_wrapper(self._bindings[name])
        ).bind()

    def _amqp_wrapper(self, consumer: Consumer) -> Callable[[DeliveredMessage], Awaitable[None]]:
        async def _wrapper(message: DeliveredMessage) -> None:
            if not (data := self.prepared_data(consumer, message.body)):
                log.debug('Calling %s by %s without data', consumer, consumer.queue.name)
                return

            log.debug('Calling %s by %s with %s', consumer,
                consumer.queue.name, str(data).encode('utf-8'))
            timer = time.monotonic()

            try:
                data['amqp'] = message
                await asyncio.wait_for(consumer.handler(**data), consumer.timeout)

                if consumer.options.get('autoack', True) and message.delivery_tag:
                    await message.channel.basic_ack(delivery_tag=message.delivery_tag)

            except TypeError:
                log.exception('Call %s failed', consumer)

            except asyncio.TimeoutError:
                log.fatal('TimeoutError: %s %.2f', consumer, time.monotonic() - timer)

                if message.delivery_tag:
                    await message.channel.basic_nack(delivery_tag=message.delivery_tag)

        return _wrapper

    async def declare_queue(self, name: str, **options: Any) -> None:
        '''
            Declare queue with queue_declare method.

            :param name: string, queue name
            :param \*\*options: other queue_declare options
        '''  # noqa: W605

        channel = await self.get_channel()
        info = await channel.queue_declare(name, **options)
        self.log.info('Declare/get queue "%(queue)s" with %(message_count)s '
            'messages, %(consumer_count)s consumers', info)

    async def queue_length(self, name: str, **options: Any) -> int:
        '''
            Get a queue length with queue_declare method.

            :param name: string, queue name
        '''

        channel = await self.get_channel()
        info = await channel.queue_declare(name)

        if isinstance(info['message_count'], int | str | bytes):
            return int(info['message_count'])

        return 0

    @staticmethod
    async def putout(amqp: DeliveredMessage) -> None:
        '''
            Send acknowledgement to broker with basic_client_ack

            :param amqp: pamqp.DeliveredMessage
        '''
        if amqp.delivery_tag:
            await amqp.channel.basic_ack(delivery_tag=amqp.delivery_tag)


class ReConnection(Connection):
    ''' AMQP connection with close callback '''

    def __init__(self, close_callback: Callable, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.close_callback = close_callback

    async def _on_close(self, ex: ExceptionType | None = ConnectionClosedDefault) -> None:
        try:
            await super()._on_close(ex)
        finally:
            await self.close_callback()


@dataclass(slots=True)
class ManagedConnection:
    ''' Binded connection with rebind logic '''
    dsn: str
    queue_name: str
    handler: Callable
    bind_attempts: int = 0
    bind_running: bool = False

    async def bind(self) -> None:
        ''' Start connection and bind consumer '''
        connection = ReConnection(self.rebind, self.dsn)
        await connection.connect()
        channel = await connection.channel()
        log.warning('Declare queue "%s"', self.queue_name)
        await channel.queue_declare(self.queue_name)
        await channel.basic_consume(self.queue_name, self.handler)

    async def rebind(self) -> bool:
        if self.bind_running:
            log.exception('Already rebinding queue "%s"', self.queue_name)
            return False

        if self.bind_attempts > REBIND_ATTEMPTS:
            log.exception('Failed all attempts to rebind queue "%s"', self.queue_name)
            return False

        await asyncio.sleep((self.bind_attempts ** 2) * REBIND_BASE_DELAY)
        self.bind_attempts += 1

        try:
            await self.bind()
            log.info('Success rebind queue "%s"', self.queue_name)
            self.bind_attempts = 0
            return True

        except (AMQPError, OSError) as exc:
            log.exception('Failed rebind queue "%s": %s', self.queue_name, exc)
            asyncio.create_task(self.rebind())
            return False

        finally:
            self.bind_running = False
