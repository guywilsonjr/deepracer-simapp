import base64
import os
import multiprocessing.queues
from io import BytesIO
from typing import Any, Optional, Tuple

import numpy as np
import orjson
from PIL import Image
import standard_processor


socket_path = os.environ['SIDECAR_SOCKET_PATH']


def generate_image_message(
    image_data: bytes,
    sim_id: int,
    rollout_idx: int,
    rollout_step: int,
    episode: int,
    steps: int,
    sequence: int
) -> bytes:
    msg = {
        'message_type': 'IMAGE',
        'sim_id': sim_id,
        'rollout_idx': rollout_idx,
        'rollout_step': rollout_step,
        'episode': episode,
        'steps': steps,
        'sequence': sequence,
        'b64_image': base64.b64encode(image_data).decode()
    }
    return orjson.dumps(msg)


def process_image_message(mp4_video_metrics_info, major_cv_image) -> Optional[bytes]:
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
            byte_image = BytesIO()
            Image.fromarray(major_cv_image).save(byte_image, format='PNG')
            return generate_image_message(image_data=byte_image.getvalue(), sim_id=sim_id, rollout_idx=rollout_idx, rollout_step=rollout_step, episode=episode, steps=steps, sequence=sequence)
    return None


def image_queue_consumer(image_queue: 'multiprocessing.queues.SimpleQueue[Tuple[Any, np.ndarray]]', message_queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    while True:
        message = image_queue.get()
        if message is None:
            break
        try:
            processed_message = process_image_message(message[0], message[1])
        except Exception as e:
            standard_processor.logger.error(f"Error processing image message: {e}")
            continue
        if processed_message is not None:
            message_queue.put(processed_message)


def run(image_queue: 'multiprocessing.queues.SimpleQueue[Tuple[Any, np.ndarray]]', message_queue: 'multiprocessing.queues.SimpleQueue[bytes]') -> None:
    standard_processor.logger.info(f"Starting Image queue consumer")
    image_queue_consumer(image_queue, message_queue)

