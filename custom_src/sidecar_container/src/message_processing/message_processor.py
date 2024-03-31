import asyncio
import logging
import multiprocessing
from typing import Any, Dict, List, Union
from . import unix_requests
import orjson


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
valid_message_types = frozenset({'checkpoint', 'reward_function', 'STEP', 'EPISODE_START'})
PRIMITIVE = Union[str, int, float, bool, None]
PRIMITIVE_DICT = Union[Dict[str, Any], PRIMITIVE]
JSONTYPE = Union[PRIMITIVE_DICT, List[Union[PRIMITIVE_DICT, List[Any]]]]


async def route_message(json_message: JSONTYPE) -> None:
    logger.info('Routing message')
    if not (isinstance(json_message, dict) or isinstance(json_message, list)):
        logger.info('Skipping due to invalid message type: {}'.format(type(json_message)))
        return
    if 'message_type' not in json_message:
        logger.info('Skipping due to Message type not found in message: {}'.format(json_message))
        return
    message_type = json_message['message_type']
    if message_type not in valid_message_types:
        logger.info('Skipping due to invalid message type: {}'.format(json_message))
        return
    logger.info('Routing message of type: {}'.format(message_type))
    return await unix_requests.post(resource_path=message_type.lower(), data=json_message)



async def queue_consumer(queue: multiprocessing.SimpleQueue) -> None:
    while True:
        message = queue.get()
        logger.info('MessageProcessor: Got message of length: {}'.format(len(message)))
        if message is None:
            logger.info('MessageProcessor: Breaking: {}'.format(len(message)))

            break
        try:
            logger.info('MessageProcessor: Decoding message')
            json_message = orjson.loads(message)
            if not json_message:
                logger.info('MessageProcessor: Skipping empty message')
                continue

            if isinstance(json_message, list):
                logger.info('MessageProcessor: Await Routing multiple messages')
                coros = [route_message(msg) for msg in json_message]
                await asyncio.gather(*coros)
                logger.info('MessageProcessor: Routed multiple messages')

            else:
                logger.info('MessageProcessor: Routing single message')
                await route_message(json_message)
        except orjson.JSONDecodeError as e:
            logger.error(f'Error decoding message: {message}\n{e}')
        except Exception as e:
            logger.error(f'Error routing message: {message}\n{e}')


def run(queue: multiprocessing.SimpleQueue) -> None:
    logger.info('Starting message processor')
    asyncio.run(queue_consumer(queue))
