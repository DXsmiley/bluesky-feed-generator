import asyncio
from datetime import datetime, timedelta
from foxfeed.database import make_database_connection
from foxfeed.algos.generators import LOOKBACK_HARD_LIMIT
from foxfeed.metrics import METRICS_MAXIMUM_LOOKBACK


POST_MAX_AGE = timedelta(days=60)


async def main():
    now = datetime.utcnow()
    db = await make_database_connection(timeout=120)

    print('Cleaning up the database...')

    postscore_max_version = await db.postscore.find_first(order={'version': 'desc'})
    if postscore_max_version is not None:
        c = await db.postscore.delete_many(where={'version': {'not': postscore_max_version.version}})
        print('Deleted', c, 'postscores')

    c = await db.servedblock.delete_many(
        where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}}
    )
    print('Deleted', c, 'servedblocks')

    c = await db.servedpost.delete_many(
        where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}}
    )
    print('Deleted', c, 'servedposts')

    c = await db.blueskyclientsession.delete_many(
        where={'created_at': {'lt': now - timedelta(days=7)}}
    )
    print('Deleted', c, 'blueskyclientsessions')

    # Can't do this while we're trying to complete our graph trees
    c = await db.like.delete_many(where={'created_at': {'lt': now - POST_MAX_AGE}})
    print('Deleted', c, 'likes')

    c = await db.post.delete_many(where={'indexed_at': {'lt': now - POST_MAX_AGE}})
    print('Deleted', c, 'posts')


asyncio.run(main())
