import atproto
from server.config import HANDLE, PASSWORD

AsyncClient = atproto.AsyncClient

async def make_bsky_client():
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)
    return client
