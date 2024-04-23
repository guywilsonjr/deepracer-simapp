import json
import time
from datetime import datetime
from multiprocessing import Process, Manager
from typing import Any, Dict, Optional, Tuple, Union

import numpy as np
import orjson

import standard_processor, image_processor


class SidecarProcessTracker:
    def __init__(self) -> None:
        self.started = True
        self.start_time = time.time()
        self.last_checkpoint_time = self.start_time
        self.checkpoints = {self.start_time: 0}

    def update(self, bytes_processed: int) -> None:
        self.last_checkpoint_time = time.time()
        self.checkpoints[self.last_checkpoint_time] = bytes_processed

    @property
    def total_bytes_processed(self) -> int:
        return sum(self.checkpoints.values())

    @property
    def total_time(self) -> float:
        return max(self.checkpoints.keys()) - self.start_time

    @property
    def total_throughput(self) -> float:
        return self.total_bytes_processed / self.total_time


class SidecarProcess:
    standard_process: Optional[Process]
    image_process: Optional[Process]
    standard_queue: 'SimpleQueue[bytes]'
    image_queue: 'SimpleQueue[Tuple[Any, np.ndarray]]'
    memory: Dict[str, Any]

    def __init__(self) -> None:
        self.manager = Manager()
        self.standard_process = None
        self.standard_queue = self.manager.Queue()
        self.image_process = None
        self.image_queue = self.manager.Queue()
        self.memory = {}
        self.tracker = SidecarProcessTracker()
        self.started = False

    def start_sidecar_process(self) -> None:
        '''Start the sidecar process'''
        self.standard_process = Process(target=standard_processor.run, args=(self.standard_queue,), name='sidecar_process')
        self.standard_process.start()
        self.image_process = Process(target=image_processor.run, args=(self.image_queue, self.standard_queue), name='sidecar_image_process')
        self.image_process.start()
        self.started = True

    def check_start(self):
        if not self.started:
            raise Exception('Sidecar process not started')

    def send_dated_message(self, data: Union[Dict[str, Any]]) -> None:
        self.check_start()
        if data['message_type'] != 'WORKER_START':
            if 'metadata' in data.keys():
                del data['metadata']
            if 'hyperparameters' in data.keys():
                del data['hyperparameters']
        date_time = datetime.utcnow().isoformat()
        data['date_time'] = date_time
        try:
            dump_data = orjson.dumps(data)
        except:
            dump_data = json.dumps(data).encode()

        self.tracker.update(len(dump_data))
        self.standard_queue.put(dump_data)

    def prepare_and_send_image_message(self, mp4_video_metrics_info, major_cv_image):
        self.check_start()
        self.image_queue.put((mp4_video_metrics_info, major_cv_image))



sidecar_process = SidecarProcess()

