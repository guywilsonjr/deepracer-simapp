import asyncio
import base64
import io
import logging
import os
import tarfile
from collections import defaultdict
from datetime import date

from utils import logger

import aioboto3
import uvicorn
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from http_server.sns import BufferedSNSClient

app = FastAPI()

socket_path = os.environ['SIDECAR_HTTP_SOCKET_PATH']
SIM_IMAGE_BUCKET_NAME = os.environ['SIM_IMAGE_BUCKET_NAME']
rollout_image_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
s3_session = aioboto3.Session(
    aws_access_key_id=os.environ['SNS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['SNS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['SNS_SESSION_TOKEN'],
    region_name=os.environ['AWS_DEFAULT_REGION']
)
sns_client = BufferedSNSClient()


async def upload_image_tarfile_to_s3(tdata: io.BytesIO, sim_id: int, rollout_idx: int, rollout_step: int):
    rval = tdata.getvalue()
    rsize = len(rval)
    logger.info(f"Uploading {rsize} bytes to s3")
    todays_date_str = date.today().isoformat()
    async with s3_session.client('s3') as s3:
        key = f'{todays_date_str}/{sim_id}/{rollout_idx}/{rollout_step}.tar'
        await s3.put_object(Bucket=SIM_IMAGE_BUCKET_NAME, Key=key, Body=rval)


def get_bytes_and_tar_info(image_data: bytes, split_data: list[str]):
    tar_info = tarfile.TarInfo(f'{split_data[3]}/{split_data[4]}/{split_data[5]}.png')
    tar_info.size = len(image_data)
    image_bytes = io.BytesIO(image_data)
    image_bytes.seek(0)
    return tar_info, image_bytes


async def process_rollout_step_messages(body: dict):
    # Wait for 60 seconds after the next rollout step to ensure all images are received for the previous rollout step
    await asyncio.sleep(180)
    if rollout_image_data:
        sim_id = int(body['sim_id'])
        rollout_idx = int(body['rollout_idx'])
        rollout_step = int(body['rollout_step'])
        prev_rollout_step_key = f"{sim_id}-{rollout_idx}-{rollout_step - 1}"
        prev_rollout_episode_image_data_dict = rollout_image_data[prev_rollout_step_key]
        logger.info(f"Found rollout keys: {({k:len(v) for k, v in rollout_image_data.items()})}")
        logger.info(f'Found episode keys: {({k: sorted(list(v.keys())) for k,v in prev_rollout_episode_image_data_dict.items()})}')
        if prev_rollout_episode_image_data_dict:
            num_episodes = len(prev_rollout_episode_image_data_dict)
            logger.info(f"Processing images from rollout step: {prev_rollout_step_key}. Found {num_episodes} episodes")
            image_data_dict = [
                get_bytes_and_tar_info(image_data, split_data)
                for episode_image_data_dict in prev_rollout_episode_image_data_dict.values() # For Each episode
                for step_image_data_dict in episode_image_data_dict.values() # For Each step
                for image_key, image_data in step_image_data_dict.items() # For Each image
                for split_data in [image_key.split('-')] # Get the split data as a list
            ]
            tdata = io.BytesIO()
            rollout_image_tarfile = tarfile.TarFile(name=f'{prev_rollout_step_key}.tar', mode='w', fileobj=tdata)
            [rollout_image_tarfile.addfile(tarinfo=tarinfo, fileobj=image_byteio) for tarinfo, image_byteio in image_data_dict]
            await upload_image_tarfile_to_s3(tdata, sim_id, rollout_idx, rollout_step)
        del rollout_image_data[prev_rollout_step_key]


@app.post("/sns")
async def send_to_sns(request: Request):
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    await sns_client.send(body)


@app.post("/rollout")
async def process_rollout(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(process_rollout_step_messages, body)
    await sns_client.send(body)


@app.post("/images")
async def store_images_body(request: Request):
    body = await request.json()
    sim_id = int(body['sim_id'])
    rollout_idx = int(body['rollout_idx'])
    rollout_step = int(body['rollout_step'])
    episode = int(body['episode'])
    steps = int(body['steps'])
    sequence = int(body['sequence'])
    rollout_step_key = f"{sim_id}-{rollout_idx}-{rollout_step}"
    key = f"{sim_id}-{rollout_idx}-{rollout_step}-{episode}-{steps}-{sequence}"
    b64_image_data = body['b64_image']
    image_data = base64.b64decode(b64_image_data.encode())
    rollout_image_data[rollout_step_key][episode][steps][key] = image_data



def main():
    uvicorn.run(
        app,
        host="localhost",
        uds=socket_path,
        loop='uvloop',
        http='httptools',
        log_level=logging.WARNING,
        interface='asgi3'
    )

