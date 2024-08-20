import asyncio
import os

from utils import logger
from datetime import datetime
from multiprocessing import Process, SimpleQueue
from typing import Optional


# Define the path for the Unix socket
sock_path = os.environ['SIDECAR_SOCKET_PATH']



class SidecarUnixServer:
    def __init__(self, message_queue: SimpleQueue) -> None:
        self.message_queue = message_queue

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read()
        self.message_queue.put(data)
        writer.close()

    async def async_main(self, socket_path: str) -> None:
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

async def async_main(server):
    await asyncio.gather(
        server.async_main(sock_path),
        server.async_main(sock_path),
        server.async_main(sock_path),
        server.async_main(sock_path)
    )
def main(message_queue: SimpleQueue):
    server = SidecarUnixServer(message_queue)
    asyncio.run(async_main(server))


