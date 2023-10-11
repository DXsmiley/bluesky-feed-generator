import asyncio
from foxfeed.database import make_database_connection
from foxfeed.bsky import make_bsky_client
from foxfeed import config


async def main():
    db = await make_database_connection()
    for h, p in [(config.HANDLE, config.PASSWORD), (config.PERSONAL_HANDLE, config.PERSONAL_PASSWORD)]:
        client = await make_bsky_client(db, h, p)
        print(f'{h}:')
        if client.me is None:
            print('client.me is None')
        else:
            print(client.me.model_dump_json(indent=4))


if __name__ == '__main__':
    asyncio.run(main())
