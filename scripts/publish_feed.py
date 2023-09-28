#!/usr/bin/env python3
# YOU MUST INSTALL ATPROTO SDK
# pip3 install atproto

import os
import asyncio
import dotenv
from datetime import datetime
from atproto.xrpc_client.models import ids
from atproto.xrpc_client.models.blob_ref import BlobRef
from atproto import AsyncClient, models
from typing import Optional
from foxfeed.algos.feeds import algo_details, environment_variable_name_for
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client
import io
from PIL import Image

dotenv.load_dotenv()

# YOUR bluesky handle
# Ex: user.bsky.social
HANDLE: str = os.environ['HANDLE']

# YOUR bluesky password, or preferably an App Password (found in your client settings)
# Ex: abcd-1234-efgh-5678
PASSWORD: str = os.environ['PASSWORD']

# The hostname of the server where feed server will be hosted
# Ex: feed.bsky.dev
HOSTNAME: str = os.environ['HOSTNAME']


# (Optional). Only use this if you want a service did different from did:web
SERVICE_DID: Optional[str] = None


def load_image_and_scale(path: str, max_sidelength: int) -> bytes:
    img = Image.open(path)
    w, h = img.size
    scale_factor = max_sidelength / max(w, h)
    if scale_factor > 1:
        img = img.resize((int(w * scale_factor), int(h * scale_factor)))
    bts = io.BytesIO()
    img.save(bts, format='PNG')
    return bts.getvalue()


async def register(client: AsyncClient, record_name: str, display_name: str, description: str, avatar_blob: Optional[BlobRef], alive: bool) -> str:
    feed_did = SERVICE_DID if SERVICE_DID is not None else f'did:web:{HOSTNAME}'

    if alive:

        response = await client.com.atproto.repo.put_record(
            models.ComAtprotoRepoPutRecord.Data(
                repo=client.me.did,
                collection=ids.AppBskyFeedGenerator,
                rkey=record_name,
                record=models.AppBskyFeedGenerator.Main(
                    did=feed_did,
                    displayName=display_name,
                    description=description,
                    avatar=avatar_blob,
                    createdAt=client.get_current_time_iso()
                )
            )
        )

        return response.uri

    else:

        await client.com.atproto.repo.delete_record(
            models.ComAtprotoRepoDeleteRecord.Data(
                repo=client.me.did,
                collection=ids.AppBskyFeedGenerator,
                rkey=record_name,
            )
        )

        return 'deleted'


async def main():
    db = await make_database_connection()
    client = await make_bsky_client(db)

    avatar_path = './static/logo.png'

    avatar_blob = None
    if avatar_path:
        avatar_data = load_image_and_scale(avatar_path, 128)
        avatar_blob = (await client.com.atproto.repo.upload_blob(avatar_data, timeout=30)).blob

    for i in algo_details:
        uri = await register(client, i['record_name'], i['display_name'], i['description'], avatar_blob, i['enable'])
        env_variable_name = environment_variable_name_for(i['record_name'])
        print(f'{env_variable_name}="{uri}"')


if __name__ == '__main__':
    asyncio.run(main())
