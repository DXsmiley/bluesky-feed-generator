import asyncio
from datetime import datetime, timedelta
from foxfeed.database import make_database_connection
from foxfeed.algos.generators import LOOKBACK_HARD_LIMIT
from foxfeed.metrics import METRICS_MAXIMUM_LOOKBACK

from typing import Awaitable


POST_MAX_AGE = timedelta(days=60)


async def drop(description: str, f: Awaitable[int]) -> None:
    start = datetime.now()
    print(description)
    num_rows = await f
    end = datetime.now()
    seconds = (end - start).total_seconds()
    print(f'> dropped {num_rows} rows in {seconds:.0f} seconds')


async def main():
    now = datetime.utcnow()
    db = await make_database_connection(timeout=300)

    print('Cleaning up the database...')

    postscore_max_version = await db.postscore.find_first(order={'version': 'desc'})
    if postscore_max_version is not None:
        await drop(
            'Deleting postscores',
            db.postscore.delete_many(where={'version': {'not': postscore_max_version.version}})
        )

    await drop(
        'Deleting servedblocks',
        db.servedblock.delete_many(where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}})
    )

    await drop(
        'Deleting servedposts',
        db.servedpost.delete_many(where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}})
    )

    await drop(
        'Deleting blueskyclientsessions',
        db.blueskyclientsession.delete_many(where={'created_at': {'lt': now - timedelta(days=7)}})
    )

    # Can't do this while we're trying to complete our graph trees
    await drop(
        'Deleting old likes',
        db.like.delete_many(where={'created_at': {'lt': now - POST_MAX_AGE}})
    )

    await drop(
        'Deleting old posts',
        db.post.delete_many(where={'indexed_at': {'lt': now - POST_MAX_AGE}})
    )

    await drop(
        'Deleting old likes from accounts outside the main cluster',
        db.like.delete_many(
            where={
                'created_at': {'lt': now - LOOKBACK_HARD_LIMIT * 2},
                'liker': {
                    'is': {
                        'OR': [
                            {'is_external_to_network': True},
                            {'manual_include_in_fox_feed': False},
                            {'flagged_for_manual_review': True},
                            {'is_muted': True},
                        ]
                    }
                }
            }
        )
    )

    await drop(
        'Deleting old posts from accounts outside the main cluster',
        db.post.delete_many(
            where={
                'indexed_at': {'lt': now - LOOKBACK_HARD_LIMIT * 2},
                'author': {
                    'is': {
                        'OR': [
                            {'is_external_to_network': True},
                            {'manual_include_in_fox_feed': False},
                            {'flagged_for_manual_review': True},
                            {'is_muted': True},
                        ]
                    }
                }
            }
        )
    )

asyncio.run(main())
