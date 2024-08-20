import base64
import socket
import os
import logging
import time
from queue import Queue
from typing import Any, Dict

import orjson


socket_path = os.environ['SIDECAR_SOCKET_PATH']



logger = logging.getLogger(__name__)

logger.info(f"imported sidecar standard processor")

class Bytecounter:
    def __init__(self) -> None:
        self.start_time = time.time()
        self.bytes_sent = 0
        self.checkpoint = time.time() + 10

    def increment(self, bytes_sent: int) -> None:
        self.bytes_sent += bytes_sent
        if time.time() > self.checkpoint:
            logger.info(f"Sent {self.bytes_sent * 1e-6} MB in {time.time() - self.start_time} seconds with throughput {self.throughput*1e-6} MB/sec")
            self.checkpoint = time.time() + 10

    @property
    def throughput(self) -> float:
        return self.bytes_sent / (time.time() - self.start_time)


my_bytecounter = Bytecounter()


def handle_image_message(image_data: dict) -> bytes:
    image_data_dict = image_data
    if image_data_dict['message_type'] != 'IMAGE':
        return orjson.dumps(image_data)

    byte_image = image_data_dict['image_val']
    del image_data_dict['image_val']

    b64_image = base64.b64encode(byte_image)
    image_data_dict['b64_image'] = b64_image.decode()
    return orjson.dumps(image_data_dict)


def send_unix_socket_message(message: bytes) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        try:
            num_bytes = len(message)
            logger.debug('Sending {} bytes to {}'.format(num_bytes, socket_path))
            client_socket.setblocking(True)
            client_socket.connect(socket_path)
            # Send the message
            client_socket.sendall(message)
            my_bytecounter.increment(num_bytes)
        except socket.error as e:
            logger.error(f"Socket error connecting to {socket_path}\n{e}")


def standard_queue_consumer(queue: 'Queue[Dict[str, Any]]') -> None:
    """Consumer function that reads from a queue and sends messages to a Unix socket."""
    # Ensure the socket path does not already exist

    while True:

        # Receive message from the queue
        message = queue.get()
        if message is None:
            # Use a sentinel value (like None) to indicate shutdown
            break
        serialized_message = handle_image_message(message)
        send_unix_socket_message(serialized_message)


def run(simple_queue: 'Queue[Dict[str, Any]]') -> None:
    logger.info(f"Starting queue consumer with Unix socket connection: {socket_path}")
    standard_queue_consumer(simple_queue)

