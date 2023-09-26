import asyncio
import json
import sys
from foxfeed.database import make_database_connection


async def main(filename: str):
    db = await make_database_connection()
    blob = json.load(open(filename))
    for post in blob:
        text: str = post['text']
        print('Scheduling:', text.replace('\n', ' / '))
        await db.scheduledpost.create(
            data={'text': text}
        )


if __name__ == '__main__':
    assert len(sys.argv) == 2
    filename = sys.argv[1]
    asyncio.run(main(filename))