from datetime import datetime
from typing import Optional, List, Callable, Coroutine, Any

from server.database import Database

from typing_extensions import TypedDict

from prisma.types import PostWhereInput, PostScoreWhereInput, PostScoreOrderByInput


class FeedItem(TypedDict):
    post: str # ???


class HandlerResult(TypedDict):
    cursor: Optional[str]
    feed: List[FeedItem]


from server.algos.score_task import LOOKBACK_HARD_LIMIT


HandlerType = Callable[[Database, Optional[str], int], Coroutine[Any, Any, HandlerResult]]


def chronological_feed(post_query_filter: PostWhereInput) -> HandlerType:

    async def handler(db: Database, cursor: Optional[str], limit: int) -> HandlerResult:

        if cursor:
            cursor_parts = cursor.split('::')
            if len(cursor_parts) != 2:
                raise ValueError('Malformed cursor')

            indexed_at_str, cid = cursor_parts
            indexed_at = datetime.fromtimestamp(int(indexed_at_str) / 1000)
            where: PostWhereInput = {
                'AND': [
                    {'indexed_at': {'lt': indexed_at}},
                    {'cid': {'lt': cid}},
                    post_query_filter,
                ]
            }
        else:
            where = post_query_filter

        # No replies
        where = {
            'AND': [
                where,
                {'reply_root': None}
            ]
        }

        posts = await db.post.find_many(
            take=limit,
            where=where,
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
        )

        feed: List[FeedItem] = [{'post': post.uri} for post in posts]
        cursor = f'{int(posts[-1].indexed_at.timestamp() * 1000)}::{posts[-1].cid}' if posts else None

        return {
            'cursor': cursor,
            'feed': feed
        }

    return handler


def algorithmic_feed(feed_name: str) -> HandlerType:

    async def handler(db: Database, cursor: Optional[str], limit: int) -> HandlerResult:

        if cursor is None:
            cursor_max_version = await db.postscore.find_first(order={'version': 'desc'})
            cursor_version = 0 if cursor_max_version is None else cursor_max_version.version
            cursor_offset = 0
        else:
            cursor_version_str, cursor_offset_str = cursor.split('::')
            cursor_version = int(cursor_version_str)
            cursor_offset = int(cursor_offset_str)

        posts = await db.postscore.find_many(
            take=limit,
            skip=cursor_offset,
            order={'score': 'desc'},
            where={
                'AND': [
                    {'version': {'equals': cursor_version}},
                    {'feed_name': {'equals': feed_name}},
                ]
            }
        )

        new_cursor = f'{cursor_version}::{cursor_offset + len(posts)}' if posts else None
        feed: List[FeedItem] = [{'post': post.uri} for post in posts]

        return {'cursor': new_cursor, 'feed': feed}

    return handler


fox_feed = algorithmic_feed('fox-feed')
vix_feed = algorithmic_feed('vix-feed')
fresh_feed = algorithmic_feed('fresh-feed')

fursuit_feed = chronological_feed(
    {
        'AND': [
            {'author': {'is': {'in_vix_feed': {'equals': True}}}},
            {'media_count': {'gt': 0}},
            {'mentions_fursuit': True},
        ]
    }
)
