import socket
import os
import multiprocessing.queues
import logging

from tenacity import retry, stop_after_attempt, wait_fixed


socket_path = os.environ['SIDECAR_SOCKET_PATH']

from markov.log_handler.logger import Logger


logger = Logger(__name__, logging.INFO).get_logger()

logger.info(f"imported sidecar processor")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def send_unix_socket_message(message: bytes) -> None:
    # Connect to the Unix socket
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        try:
            logger.info('Sending {} bytes to {}'.format(len(message), socket_path))
            client_socket.connect(socket_path)
            # Send the message
            client_socket.sendall(message)
        except socket.error as e:
            logger.error(f"Socket error connecting to {socket_path}\n{e}")


def queue_consumer(queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    """Consumer function that reads from a queue and sends messages to a Unix socket."""
    # Ensure the socket path does not already exist

    while True:
        logger.info('Queue consumer waiting for message')
        # Receive message from the queue
        message = queue.get()
        if message is None:
            # Use a sentinel value (like None) to indicate shutdown
            break




def run(queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    logger.info(f"Starting queue consumer with Unix socket connection: {socket_path}")
    queue_consumer(queue)

