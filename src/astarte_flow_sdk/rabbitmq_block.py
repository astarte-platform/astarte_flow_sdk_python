from abc import ABC, abstractmethod
import asyncio
import aio_pika
import os
from threading import Thread
from .block import Block, UnrecoverableQueueError
from .config import RabbitMQConfig
from .message import Message


class RabbitMQBlock(Block):
    def __init__(self,
                 config_path="/config/block/config.json",
                 rabbitmq_config_path="/config/worker/rabbitmq.conf"):
        super().__init__(config_path)

        self.__rabbitmq_config = RabbitMQConfig(rabbitmq_config_path)

        self.__producer_exchange_name = self.__rabbitmq_config.get_rabbitmq_producer_exchange(
        )["name"]
        self.__producer_exchange_routing_key = self.__rabbitmq_config.get_rabbitmq_producer_exchange(
        )["routingKey"]
        self.__producer_exchange_type = self.__rabbitmq_config.get_rabbitmq_producer_exchange(
        ).get("type", aio_pika.ExchangeType.DIRECT)

    async def run_async(self):
        self.__connection = await aio_pika.connect_robust(
            self.__rabbitmq_config.get_rabbitmq_url())

        # Creating channel
        self.__channel = await self.__connection.channel()

        # Set up producer
        if self.__producer_exchange_name:
            self.__exchange = await self.__channel.declare_exchange(
                self.__producer_exchange_name,
                self.__producer_exchange_type,
                passive=True)
        else:
            self.__exchange = self.__channel.default_exchange

        queues = self.__rabbitmq_config.get_rabbitmq_consumer_queues()

        if queues != []:
          # Start consuming
          await asyncio.gather(*[
              self.__receiver_loop_for(queue)
              for queue in queues
          ])
        else:
          # If we don't have queues, we must have this endless loop
          # otherwise asyncio terminates immediately
          while True:
            await asyncio.sleep(1)

    async def _send_message_impl(self, message: Message):
        message_bytes = bytes(message.marshal_flow_payload(), encoding='utf8')
        rmq_message = aio_pika.Message(message_bytes)
        await self.__exchange.publish(
            rmq_message, routing_key=self.__producer_exchange_routing_key)

    async def __receiver_loop_for(self, queue_name):
        async with self.__connection:
            # Declaring queue
            queue = await self.__channel.declare_queue(queue_name,
                                                       passive=True)

            async with queue.iterator() as queue_iter:
                # Cancel consuming after __aexit__
                async for message in queue_iter:
                    async with message.process():
                        try:
                            self._handle_received_message(message.body)
                        except UnrecoverableQueueError:
                            print(
                                "Received Unrecoverable Queue error, terminating receiver loop"
                            )
