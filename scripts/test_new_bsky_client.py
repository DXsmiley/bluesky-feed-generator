import asyncio
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client, get_followers

async def main():
    db = await make_database_connection()
    client = await make_bsky_client(db)
    async for i in get_followers(client, client.me.did):
        print(i.handle, i.display_name)

if __name__ == '__main__':
    asyncio.run(main())

