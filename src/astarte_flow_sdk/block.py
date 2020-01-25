from abc import ABC, abstractmethod
import asyncio
import aio_pika
import os
from threading import Thread
from .config import BlockConfig
from .message import Message


class UnrecoverableQueueError(Exception):
    """Raise this exception when processing a message to stop processing from
   a queue"""
    pass


class Block(ABC):
    """ Block represents the base class for a Flow Block. It does not implement
    any messaging-specific logic, which is instead delegated to subclasses.
    However, it defines how a Block runs and in which way developers can
    act on blocks to integrate them in their application.

    Blocks are self-contained, and implement a callback for receiving an
    ordered stream of data, and a function to send a Flow message back
    to the messaging backend. From a Developer standpoint, just the
    on_message_received callback must be implemented.

    The most common use case for a Block is standalone execution. This is
    also the default of the base scaffolding. The most typical main
    function would look something like this, assuming you want to use
    RabbitMQ as the messaging backend:

    .. code-block:: python

        # Instantiate and start a RabbitMQ Block
        block = RabbitMQBlock()
        block.set_on_message_received_callback(on_message_received)

        # Run the Block
        block.run()

    This will start the block's event loop and run until it fails or
    ends.

    run_async and start_threaded allow executing a block either in
    an existing asyncio application, or in a separate thread. They
    should be used in case you want to integrate a Block inside an
    application which will have more logic beyond the callback
    itself. Most users will be just fine with run().
    """
    def __init__(self, config_path="/config/block/config.json"):
        """ Creates a new block instance.

        :param config_path: path to the block configuration file. Defaults to
            the container's default mount path for block configuration.
        """
        super().__init__()

        self.__block_config = BlockConfig(config_path)
        self.__on_message_received_callback = None

    def set_on_message_received_callback(self, callback):
        """ Sets the on_message_received callback

        The callback's signature must be:

        .. code-block:: python

            def on_message_received(message, block):

        message is a bytes object containing the message
        block is the block instance that received the message

        :param config_path: path to the block configuration file. Defaults to
            the container's default mount path for block configuration.
        """
        self.__on_message_received_callback = callback

    def run(self):
        self.__threaded = False
        asyncio.get_event_loop().run_until_complete(self.run_async())

    def start_threaded(self) -> Thread:
        self.__threaded = True
        # Create a new loop in a separate thread
        self.__loop = asyncio.new_event_loop()
        t = Thread(target=self.__start_background_loop,
                   args=(self.__loop, ),
                   daemon=True)
        t.start()
        return t

    def send_message(self, message: Message):
        if self.__threaded:
            self.__loop.create_task(self._send_message_impl(message))
        else:
            asyncio.get_event_loop().create_task(
                self._send_message_impl(message))

    @abstractmethod
    async def run_async(self):
        pass

    @abstractmethod
    async def _send_message_impl(self, message):
        pass

    def get_block_config(self):
        return self.__block_config.get_block_config()

    def __start_background_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.run_async())

    def _handle_received_message(self, message):
        if not self.__on_message_received_callback:
            return

        # Unmarshal the message
        m = Message.frompayload(message)

        if self.__threaded:
            self.__loop.call_soon_threadsafe(
                self.__on_message_received_callback, m, self)
        else:
            asyncio.get_event_loop().call_soon(
                self.__on_message_received_callback, m, self)
