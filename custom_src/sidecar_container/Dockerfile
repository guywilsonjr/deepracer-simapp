FROM 936272581790.dkr.ecr.us-west-1.amazonaws.com/cumulonimbusinfrastructurestackecrb011c8ff-pythona57b7ce0-zwlwp6r36elt:3.12
RUN python3 -m venv .venv --upgrade-deps && .venv/bin/python -m pip install -U pip setuptools wheel packaging
ADD src/requirements.txt .
RUN .venv/bin/python -m pip install -Ur requirements.txt
ADD src/ src/
RUN mkdir sockets
ENV SIDECAR_SOCKET_PATH=/sockets/sidecar.sock
ENV SIDECAR_HTTP_SOCKET_PATH=/sockets/sidecar_http.sock
#ENTRYPOINT [".venv/bin/uvicorn", "main:app", "--uds", "sockets/sidecar_container.sock"]
ENTRYPOINT [".venv/bin/python3", "src/main.py"]
