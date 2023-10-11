import asyncio
from atproto.xrpc_client.models import ids
from atproto.xrpc_client.models.blob_ref import BlobRef
from atproto import AsyncClient, models
from typing import Optional
from foxfeed.algos.feeds import algo_details
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client
from foxfeed import image
from foxfeed import config


def load_image_and_scale(path: str) -> bytes:
    img = image.open(path)
    img = image.scale_down(img, 128)
    return image.to_bytes(img, 'PNG')


async def register(client: AsyncClient, record_name: str, display_name: str, description: str, avatar_blob: Optional[BlobRef], alive: bool) -> str:
    feed_did = config.SERVICE_DID

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
    client_public = await make_bsky_client(db, config.HANDLE, config.PASSWORD)
    client_personal = await make_bsky_client(db, config.PERSONAL_HANDLE, config.PERSONAL_PASSWORD)

    d1 = load_image_and_scale('./static/logo.png')
    public_blob = (await client_public.com.atproto.repo.upload_blob(d1, timeout=30)).blob

    d2 = load_image_and_scale('./static/logo-grey.png')
    personal_blob = (await client_personal.com.atproto.repo.upload_blob(d2, timeout=30)).blob

    for i in algo_details:
        print(await register(client_public, i['record_name'], i['display_name'], i['description'], public_blob, i['show_on_main_account']))
        print(await register(client_personal, i['record_name'], i['display_name'], i['description'], personal_blob, i['show_on_personal_account']))


if __name__ == '__main__':
    asyncio.run(main())
