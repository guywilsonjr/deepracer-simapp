from datetime import datetime
from multiprocessing import Process, SimpleQueue
from typing import Any, Dict, List, Optional, Union
import logging
import orjson

from . import processor


with open('/opt/install/beenhere.txt', 'w') as f:
    f.write('sidecar.py has been run')

with open('beenhere.txt', 'w') as f:
    f.write('sidecar.py has been run')

logging.basicConfig(
    filename='/opt/install/sidecar.log',
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


class SidecarProcess:
    process: Optional[Process]
    queue: 'SimpleQueue[bytes]'
    memory: Dict[str, Any]

    def __init__(self) -> None:
        self.process = None
        self.queue = SimpleQueue()
        self.memory = {}

    def start_sidecar_process(self) -> None:
        '''Start the sidecar process'''
        self.process = Process(target=processor.run, args=(self.queue,), name='sidecar_process')
        self.process.start()

    def send_message(self, message: str) -> None:
        logger.info('Sending message to sidecar process of length: {}'.format(len(message)))
        '''Send a message to the sidecar process'''
        self.queue.put(message.encode())

    def send_data(self, data: Union[Dict[str, Any], List[Any]]) -> None:
        logger.info('Sending data to sidecar process')
        self.queue.put(orjson.dumps(data))

    def send_dated_message(self, data: Union[Dict[str, Any], List[Any]]) -> None:
        logger.info('Sending data messageto sidecar process')
        date_time = datetime.utcnow().isoformat()
        data['date_time'] = date_time
        self.queue.put(orjson.dumps(data))


sidecar_process = SidecarProcess()

