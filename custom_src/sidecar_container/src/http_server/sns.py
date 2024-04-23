import os
from collections import deque
from typing import Any, Dict

import aioboto3
import orjson

from utils import logger


SNS_MAX_MESSAGE_SIZE = 256 * 1024


class BufferedSNSClient:

    def __init__(self):
        self.cache = deque()
        self.message_bytes = None
        self.size_checkpoint = 100
        self.message_length = 0
        self.aws_session = aioboto3.Session(
            aws_access_key_id=os.environ['SNS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['SNS_SECRET_ACCESS_KEY'],
            aws_session_token=os.environ['SNS_SESSION_TOKEN'],
            region_name=os.environ['AWS_DEFAULT_REGION']
        )

    async def _send(self, message: str):
        async with self.aws_session.client('sns') as sns:
            await sns.publish(
                TopicArn=os.environ['SNS_TOPIC_ARN'],
                Message=message
            )

    async def send(self, new_message_data: Dict[str, Any]):
        new_message_bytes = orjson.dumps(new_message_data)
        new_message_size = len(new_message_bytes)

        if new_message_size >= SNS_MAX_MESSAGE_SIZE:
            raise ValueError("Individual message size exceeds SNS max message size")

        full_msg_list = list(self.cache)
        current_full_message_bytes = orjson.dumps(full_msg_list)
        if len(current_full_message_bytes) >= SNS_MAX_MESSAGE_SIZE:
            raise ValueError("Total Message size already exceeds SNS max message size")

        updated_full_msg_list = full_msg_list + [new_message_data]
        update_full_msg_bytes = orjson.dumps(updated_full_msg_list)
        update_full_msg_size = len(update_full_msg_bytes)
        if update_full_msg_size >= SNS_MAX_MESSAGE_SIZE:
            logger.info("Sending Aggregated SNS message of size: {}".format(len(current_full_message_bytes)))
            await self._send(current_full_message_bytes.decode())
            self.cache.clear()
            self.cache.append(new_message_data)
            self.message_bytes = orjson.dumps(list(self.cache))

        else:
            self.cache.append(new_message_data)
            self.message_bytes = orjson.dumps(list(self.cache))

        message_size = len(self.message_bytes)
        if message_size >= self.size_checkpoint:
            logger.info("Aggregating SNS message of size: {}".format(message_size))
            self.size_checkpoint += 1000
