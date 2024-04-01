import asyncio
import os

from utils import logger
from datetime import datetime
from multiprocessing import SimpleQueue
from typing import Optional


# Define the path for the Unix socket
sock_path = os.environ['SIDECAR_SOCKET_PATH']


class MyProtocol(asyncio.Protocol):

    def __init__(self, loop: asyncio.AbstractEventLoop, message_queue: SimpleQueue) -> None:
        self.message_queue = message_queue
        self.transport: Optional[asyncio.BaseTransport] = None
        self.on_con_lost = loop.create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if not self.transport:
            self.transport = transport
        return None

    def data_received(self, data: bytes) -> None:
        logger.info("{} Received: {} bytes".format(datetime.utcnow().isoformat(), len(data)))
        self.message_queue.put(data)
        if self.transport:
            self.transport.close()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_con_lost.set_result(True)


async def async_main(socket_path: str, message_queue: SimpleQueue) -> None:
    # Ensure the socket does not already exist
    logger.info(f"Removing existing socket at {socket_path}")
    try:
        os.unlink(socket_path)
    except OSError:
        if os.path.exists(socket_path):
            raise

    # Create the asyncio event loop
    loop = asyncio.get_running_loop()
    # Create the Unix socket server
    logger.info(f"Creating server on {socket_path}")
    server = await loop.create_unix_server(lambda: MyProtocol(loop, message_queue), path=socket_path, start_serving=True)

    async with server:
        logger.info(f"Serving on {socket_path}")
        await server.serve_forever()


def main(message_queue: SimpleQueue):
    asyncio.run(async_main(sock_path, message_queue))
