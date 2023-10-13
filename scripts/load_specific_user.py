import asyncio
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client
from foxfeed import config
from foxfeed.store import store_user
from foxfeed.bsky import get_specific_profiles


async def main():
    db = await make_database_connection()
    client = await make_bsky_client(db, config.HANDLE, config.PASSWORD)
    user = await client.app.bsky.actor.get_profile({'actor': 'onyxxfur.bsky.social'})
    async with db.tx() as tx:
        async for i in get_specific_profiles(client, [user.did], None):
            await store_user(tx, i, is_muted=False, is_furrylist_verified=False, is_external_to_network=True, flag_for_manual_review=False)
        async for i in get_specific_profiles(client, [user.did], None):
            await store_user(tx, i, is_muted=False, is_furrylist_verified=False, is_external_to_network=True, flag_for_manual_review=False)


if __name__ == '__main__':
    asyncio.run(main())
