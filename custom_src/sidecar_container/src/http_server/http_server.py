import os
from utils import logger
import aioboto3
import orjson
import uvicorn
from fastapi import FastAPI, HTTPException, Request


app = FastAPI()


checkpoints = []
socket_path = os.environ['SIDECAR_HTTP_SOCKET_PATH']
steps = []

aws_session = aioboto3.Session(
    aws_access_key_id=os.environ['SNS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['SNS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['SNS_SESSION_TOKEN'],
    region_name=os.environ['AWS_DEFAULT_REGION']
)


@app.post("/sns")
async def send_to_sns(request: Request):

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    logger.info("Received sns post request of size: {}".format(len(orjson.dumps(body))))

    async with aws_session.client('sns') as sns:
        await sns.publish(
            TopicArn=os.environ['SNS_TOPIC_ARN'],
            Message=orjson.dumps(body).decode()
        )

@app.post("/checkpoint")
async def store_checkpoint_body(request: Request):
    try:
        print("Received checkpoint store request")
        body = await request.json()
        if isinstance(body, list):
            checkpoints.extend(body)
        else:
            checkpoints.append(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/checkpoint")
async def get_checkpoint_body():
    try:
        print("Received checkpoint get request")
        return checkpoints
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/step")
async def store_step_body(request: Request):
    try:
        body = await request.json()

    except Exception as e:
        print('Error storing step body: ', e)
        raise HTTPException(status_code=400, detail=str(e))

    if isinstance(body, list):
        steps.extend(body)
    else:
        steps.append(body)


@app.get("/step")
async def get_step_body():
    try:
        print("Received step get request")

        return steps
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def main():
    uvicorn.run(app, host="localhost", uds=socket_path)

