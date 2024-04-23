from multiprocessing import Process, Manager

import pyroscope

from unix_server import server
from message_processing import message_processor
from http_server import http_server


pyroscope.configure(
    application_name="sidecar_container",  # replace this with some name for your application
    server_address="http://pyroscope:4040",  # replace this with the address of your Pyroscope server
    detect_subprocesses=True,  # detect subprocesses started by the main process; default is False
    oncpu=False,  # report cpu time only; default is True
    gil_only=False,  # only include traces for threads that are holding on to the Global Interpreter Lock; default is True
    enable_logging=True,  # does enable logging facility; default is False
    report_pid=True,
    report_thread_id=True,
    report_thread_name=True,
)



def main():
    with Manager() as manager:
        message_queue = manager.Queue()
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
