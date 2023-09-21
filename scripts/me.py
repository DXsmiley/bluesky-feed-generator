import asyncio
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client


async def main():
    db = await make_database_connection()
    client = await make_bsky_client(db)
    if client.me is None:
        print('client.me is None')
    else:
        print(client.me.model_dump_json(indent=4))


if __name__ == '__main__':
    asyncio.run(main())
