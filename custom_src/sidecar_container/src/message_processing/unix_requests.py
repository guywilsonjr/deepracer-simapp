import os
from typing import Any, Dict

import aiohttp


unix_socket_path = os.environ['SIDECAR_HTTP_SOCKET_PATH']


async def post(resource_path: str, data: Dict[str, Any]) -> None:
    connector = aiohttp.UnixConnector(path=unix_socket_path)

    async with aiohttp.ClientSession(connector=connector) as session:
        url = f"http://localhost/{resource_path}"
        async with session.post(url, json=data) as response:
            response_text = await response.text()
            print(f"Response status: {response.status}")
            print(f"Response body length: {len(response_text)}")
