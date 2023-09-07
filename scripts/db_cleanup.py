import asyncio
from datetime import datetime
from server.database import make_database_connection
from server.algos.score_task import LOOKBACK_HARD_LIMIT

async def main():
    now = datetime.utcnow()
    db = await make_database_connection(timeout=120)

    print('Cleaning up the database...')

    postscore_max_version = await db.postscore.find_first(order={'version': 'desc'})
    if postscore_max_version is not None:
        c = await db.postscore.delete_many(where={'version': {'not': postscore_max_version.version}})
        print('Deleted', c, 'postscores')

    # c = await db.post.delete_many(where={'indexed_at': {'lt': now - LOOKBACK_HARD_LIMIT}})
    # print('Deleted', c, 'posts')


asyncio.run(main())
