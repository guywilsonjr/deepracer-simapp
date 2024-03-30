import multiprocessing
import socket
import os


def queue_consumer(queue: multiprocessing.SimpleQueue, socket_path: str) -> None:
    """Consumer function that reads from a queue and sends messages to a Unix socket."""
    # Ensure the socket path does not already exist

    while True:
        # Receive message from the queue
        message = queue.get()
        if message is None:
            # Use a sentinel value (like None) to indicate shutdown
            break

        # Connect to the Unix socket
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
            try:
                client_socket.connect(socket_path)
                # Send the message
                client_socket.sendall(message.encode('utf-8'))
            except socket.error as e:
                print(f"Socket error: {e}")


def run(queue: multiprocessing.SimpleQueue) -> None:
    socket_path = os.environ['SIDECAR_SOCKET_PATH']
    queue_consumer(queue, socket_path)

