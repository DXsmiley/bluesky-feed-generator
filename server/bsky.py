import atproto
from publish_feed import HANDLE, PASSWORD # TODO: Bleh

AsyncClient = atproto.AsyncClient

async def make_bsky_client():
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)
    return client
