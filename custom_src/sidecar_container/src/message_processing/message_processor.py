import asyncio
from utils import logger
import multiprocessing
from typing import Any, Dict, List, Union
from . import unix_requests
import orjson



PRIMITIVE = Union[str, int, float, bool, None]
PRIMITIVE_DICT = Union[Dict[str, Any], PRIMITIVE]
JSONTYPE = Union[PRIMITIVE_DICT, List[Union[PRIMITIVE_DICT, List[Any]]]]

valid_message_types = frozenset(
    {
        'SIMULATION_START',
        'WORKER_START',
        'ROLLOUT_START',
        'CHECKPOINT',
        'EPISODE_IDX_FINISH',
        'EPISODE_FINISH',
        'SIM_TRACE_LOG',
        'IMAGE'
    }
)
EXCLUDE_MESSAGE_TYPES = frozenset()
message_processor_include_filters = frozenset({msg_type for msg_type in valid_message_types if msg_type not in EXCLUDE_MESSAGE_TYPES})

message_route_map = {
    'SIMULATION_START': 'sns',
    'WORKER_START': 'sns',
    'ROLLOUT_START': 'rollout',
    'CHECKPOINT': 'sns',
    'EPISODE_IDX_FINISH': 'sns',
    'EPISODE_FINISH': 'sns',
    'SIM_TRACE_LOG': 'sns',
    'IMAGE': 'images'
}

class Counter:
    def __init__(self):
        self.message_type_counter = {}
        self.message_counter = 0

    def add_message(self, message_type: str):
        if message_type in self.message_type_counter:
            self.message_type_counter[message_type] += 1
        else:
            self.message_type_counter[message_type] = 1
        self.message_counter += 1

        if self.message_counter % 100 == 0:
            logger.info('Message count: {}'.format(self.message_counter))
            logger.info('Message type count: {}'.format(self.message_type_counter))


counter = Counter()


async def route_message(json_message: JSONTYPE) -> None:
    if not isinstance(json_message, dict):
        raise ValueError('Message must be of type dict'.format(type(json_message)))
    if 'message_type' not in json_message:
        raise ValueError('Message type not found in message. Found message keys: {}'.format(json_message.keys()))

    message_type = json_message['message_type']
    if message_type not in valid_message_types:
        raise ValueError('Invalid message type found: {}'.format(message_type))
    counter.add_message(message_type)

    if message_type not in message_processor_include_filters:
        logger.debug('Skipping due to Non-Included message type: {}'.format(message_type))
        return

    resource_path = message_route_map[message_type]
    logger.debug('Routing message of type: {} to resource path: {}'.format(message_type, resource_path))

    return await unix_requests.post(resource_path=resource_path, data=json_message)



async def queue_consumer(queue: multiprocessing.SimpleQueue) -> None:
    while True:
        message = queue.get()
        logger.debug('MessageProcessor: Received message of length: {}'.format(len(message)))
        if message is None:
            break
        try:
            json_message = orjson.loads(message.decode())
            if not json_message:
                logger.info('MessageProcessor: Skipping empty message')
                continue

            if isinstance(json_message, list):
                coros = [route_message(msg) for msg in json_message]
                await asyncio.gather(*coros)
            else:
                await route_message(json_message)
        except orjson.JSONDecodeError as e:
            logger.error(f'Error decoding message of length: {len(message)}\n{e}')
        except Exception as e:
            logger.error(f'Error routing message of length: {len(message)}\n{e}')


def run(queue: multiprocessing.SimpleQueue) -> None:
    logger.info('Starting message processor')
    asyncio.run(queue_consumer(queue))
