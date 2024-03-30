from multiprocessing import Process, SimpleQueue
from uuid import uuid1

from sidecar_process import processor


class SidecarProcess:
    def __init__(self):
        self.process = None
        self.queue = SimpleQueue()
        self.memory = {}

    def start_sidecar_process(self):
        '''Start the sidecar process'''
        # Start the sidecar process
        self.process = Process(target=processor.run, args=(self.queue,), name='sidecar_process')
        self.process.start()

    def send_message(self, message: str):
        '''Send a message to the sidecar process'''
        self.queue.put(message)
        return str(uuid1())



sidecar_process = SidecarProcess()

