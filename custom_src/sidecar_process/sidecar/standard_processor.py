import socket
import os
import multiprocessing.queues
import logging
import time


socket_path = os.environ['SIDECAR_SOCKET_PATH']

from markov.log_handler.logger import Logger


logger = Logger(__name__, logging.INFO).get_logger()

logger.info(f"imported sidecar standard processor")

class Bytecounter:
    def __init__(self):
        self.start_time = None
        self.bytes_sent = 0
        self.checkpoint = time.time() + 10

    def increment(self, bytes_sent):
        if self.start_time is None:
            self.start_time = time.time()
        self.bytes_sent += bytes_sent
        if time.time() > self.checkpoint:
            logger.info(f"Sent {self.bytes_sent * 1e-9} MB in {time.time() - self.start_time} seconds with throughput {self.throughput*1e-9} MB/sec")
            self.checkpoint = time.time() + 10

    @property
    def throughput(self):
        return self.bytes_sent / (time.time() - self.start_time)

my_bytecounter = Bytecounter()


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


def standard_queue_consumer(queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    """Consumer function that reads from a queue and sends messages to a Unix socket."""
    # Ensure the socket path does not already exist

    while True:
        # Receive message from the queue
        message = queue.get()
        if message is None:
            # Use a sentinel value (like None) to indicate shutdown
            break
        send_unix_socket_message(message)


def run(simple_queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    logger.info(f"Starting queue consumer with Unix socket connection: {socket_path}")
    standard_queue_consumer(simple_queue)

