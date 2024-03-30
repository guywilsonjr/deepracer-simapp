import asyncio
import os

import logging
from typing import Optional


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# Define the path for the Unix socket
sock_path = os.environ['SIDECAR_SOCKET_PATH']


class MyProtocol(asyncio.Protocol):

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.transport: Optional[asyncio.BaseTransport] = None
        self.on_con_lost = loop.create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if not self.transport:
            self.transport = transport
        print("Connection made")

        return None

    def data_received(self, data: bytes) -> None:
        print("Received:", data.decode())
        if self.transport:
            self.transport.close()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        # The socket has been closed
        self.on_con_lost.set_result(True)



async def async_main(socket_path: str) -> None:
    # Ensure the socket does not already exist
    try:
        os.unlink(socket_path)
    except OSError:
        if os.path.exists(socket_path):
            raise

    # Create the asyncio event loop
    loop = asyncio.get_running_loop()
    # Create the Unix socket server
    server = await loop.create_unix_server(lambda: MyProtocol(loop), path=socket_path, start_serving=True)

    async with server:
        logger.info(f"Serving on {socket_path}")
        await server.serve_forever()


def main():
    asyncio.run(async_main(sock_path))


if __name__ == "__main__":
    main()