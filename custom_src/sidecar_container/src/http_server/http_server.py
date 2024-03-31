import os

import uvicorn
from fastapi import FastAPI, HTTPException, Request


app = FastAPI()


checkpoints = []
socket_path = os.environ['SIDECAR_HTTP_SOCKET_PATH']
steps = []


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
        if isinstance(body, list):
            steps.extend(body)
        else:
            steps.append(body)
    except Exception as e:
        print('Error storing step body: ', e)
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/step")
async def get_step_body():
    try:
        print("Received step get request")

        return steps
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def main():
    uvicorn.run(app, host="localhost", uds=socket_path)

