import asyncio
import os

from utils import logger
from datetime import datetime
from multiprocessing import Process, SimpleQueue
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
        logger.debug("{} Received: {} bytes".format(datetime.utcnow().isoformat(), len(data)))
        self.message_queue.put(data)
        if self.transport:
            self.transport.close()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_con_lost.set_result(True)

class SidecarUnixServer:
    def __init__(self, message_queue: SimpleQueue) -> None:
        self.message_queue = message_queue

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read()
        self.message_queue.put(data)
        writer.close()
        await writer.wait_closed()

    async def async_main(self, socket_path: str) -> None:

        # Create the asyncio event loop
        # Create the Unix socket server
        logger.info(f"Creating server on {socket_path}")
        server = await asyncio.start_unix_server(
            client_connected_cb=lambda reader, writer: self.handle_connection(reader, writer),
            limit=2 ** 32,
            backlog=2 ** 16,
            path=socket_path
        )

        async with server:
            logger.info(f"Serving on {socket_path}")
            await server.serve_forever()


def main(message_queue: SimpleQueue):
    server = SidecarUnixServer(message_queue)
    asyncio.run(server.async_main(sock_path))

