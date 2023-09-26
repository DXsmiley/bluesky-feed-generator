import asyncio
from foxfeed.database import make_database_connection


async def main():
    db = await make_database_connection()
    await db.scheduledpost.update_many(
        where={'status': 'scheduled'},
        data={'status': 'cancelled'},
    )


if __name__ == '__main__':
    asyncio.run(main())
