from queue import Queue
from datetime import datetime
from multiprocessing import Process, Manager
from typing import Any, Dict, List, Optional, Union

import numpy as np
from PIL import Image

import standard_processor

import io
import os


def generate_image_message(
    sim_id: int,
    rollout_idx: int,
    rollout_step: int,
    episode: int,
    steps: int,
    sequence: int
) -> dict:
    msg = {
        'message_type': 'IMAGE',
        'sim_id': sim_id,
        'rollout_idx': rollout_idx,
        'rollout_step': rollout_step,
        'episode': episode,
        'steps': steps,
        'sequence': sequence,
    }
    return msg


def process_image(mp4_video_metrics_info: List[str]) -> Optional[dict]:
    if len(mp4_video_metrics_info) > 0:
        mp4_video_data = mp4_video_metrics_info[0]
        data_str = str(mp4_video_data)
        entries = data_str.splitlines()
        data_dict = {entry.split(':')[0]: entry.split(':')[1] for entry in entries}

        if data_dict['sim_id']:
            sim_id = int(data_dict['sim_id'])
            rollout_idx = int(data_dict['rollout_idx'])
            rollout_step = int(data_dict['rollout_step'])
            episode = int(data_dict['episode'])
            steps = int(data_dict['steps'])
            sequence = int(data_dict['sequence'])
            return generate_image_message(sim_id=sim_id, rollout_idx=rollout_idx, rollout_step=rollout_step, episode=episode, steps=steps, sequence=sequence)
    return None


def process_image_message(
    standard_queue: 'Queue[Dict[str, Any]]',
    mp4_video_metrics_info: List[str],
    major_cv_image: np.ndarray
) -> None:
    image_data = process_image(mp4_video_metrics_info)
    if image_data:
        image_data['img_size'] = major_cv_image.shape
        byte_image_io = io.BytesIO()
        Image.fromarray(major_cv_image).save(byte_image_io, format='PNG')

        image_data['image_val'] = byte_image_io.getvalue()

        standard_queue.put(image_data)


class SidecarProcess:
    standard_process: Optional[Process]
    image_process: Optional[Process]
    standard_queue: 'Queue[Dict[str, Any]]'
    memory: Dict[str, Any]

    def __init__(self) -> None:
        self.manager = Manager()
        self.standard_process = None
        self.standard_queue = self.manager.Queue()
        self.memory = {}
        self.started = False
        self.enabled = bool(os.environ.get('ENABLE_SIDECAR', False))


    def start_sidecar_process(self) -> None:
        '''Start the sidecar process'''
        if not self.enabled:
            return
        self.standard_process = Process(target=standard_processor.run, args=(self.standard_queue,), name='sidecar_process')
        self.standard_process.start()
        self.started = True


    def check_start(self) -> None:
        if not self.started:
            raise Exception('Sidecar process not started')

    def send_dated_message(self, data: Union[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        self.check_start()
        if data['message_type'] != 'WORKER_START':
            if 'metadata' in data.keys():
                del data['metadata']
            if 'hyperparameters' in data.keys():
                del data['hyperparameters']
        if data['message_type'] == 'SIM_TRACE_LOG':
            data['yaw'] = float(data['yaw'])
        date_time = datetime.utcnow().isoformat()
        data['date_time'] = date_time

        self.standard_queue.put(data)

    def prepare_and_send_image_message(self, mp4_video_metrics_info: List[str], major_cv_image: np.ndarray) -> None:
        if not self.enabled:
            return
        self.check_start()
        Process(target=process_image_message, args=(self.standard_queue, mp4_video_metrics_info, major_cv_image)).start()


sidecar_process = SidecarProcess()
