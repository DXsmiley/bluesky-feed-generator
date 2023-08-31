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

    # c = await db.actor.delete_many(where={'in_fox_feed': False, 'in_vix_feed': False})
    # print('Deleted', c, 'actors')

    c = await db.post.delete_many(where={'indexed_at': {'lt': now - LOOKBACK_HARD_LIMIT}})
    print('Deleted', c, 'posts')

    likes = await db.like.group_by(['post_uri', 'liker_id'], count=True)
    for i in likes:
        c = i['_count']['_all']
        if c > 1:
            await db.like.delete_many(where={'post_uri': i['post_uri'], 'liker_id': i['liker_id']})
            print(i)


asyncio.run(main())
