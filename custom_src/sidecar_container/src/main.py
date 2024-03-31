from multiprocessing import Process, SimpleQueue
from unix_server import server
from message_processing import message_processor
from http_server import http_server

message_queue = SimpleQueue()


def main():
    message_processor_process = Process(target=message_processor.run, args=(message_queue,), name='sidecar_message_processor')
    unix_server_process = Process(target=server.main, args=(message_queue, ), name='sidecar_server')
    http_server_process = Process(target=http_server.main, name='http_server')

    message_processor_process.start()
    unix_server_process.start()
    http_server_process.start()

    unix_server_process.join()
    message_processor_process.join()
    http_server_process.join()


if __name__ == "__main__":
    main()
