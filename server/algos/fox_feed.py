from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional, List, Callable

from server.database import Post

from typing_extensions import TypedDict

from prisma.types import PostWhereInput

class FeedItem(TypedDict):
    post: str # ???


class HandlerResult(TypedDict):
    cursor: Optional[str]
    feed: List[FeedItem]


def chronological_feed(post_query_filter: PostWhereInput) -> Callable[[Optional[str], int], HandlerResult]:

    def handler(cursor: Optional[str], limit: int) -> HandlerResult:

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

        posts = Post.prisma().find_many(
            take=limit,
            where=where,
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
            include={'author': True}
        )

        feed: List[FeedItem] = [{'post': post.uri} for post in posts]

        cursor = None
        last_post = posts[-1] if posts else None
        if last_post:
            cursor = f'{int(last_post.indexed_at.timestamp() * 1000)}::{last_post.cid}'

        return {
            'cursor': cursor,
            'feed': feed
        }

    return handler


def algorithmic_feed(post_query_filter: PostWhereInput) -> Callable[[Optional[str], int], HandlerResult]:

    def handler(cursor: Optional[str], limit: int) -> HandlerResult:

        if cursor is None:
            cursor_starttime = datetime.now(tz=timezone.utc)
            lowest_score = 1_000_000_000
        else:
            cursor_starttime_str, lowest_score_str = cursor.split('::')
            cursor_starttime = datetime.fromtimestamp(int(cursor_starttime_str) / 1000, tz=timezone.utc)
            lowest_score = float(lowest_score_str)

        posts = Post.prisma().find_many(
            take=2000,
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
            where={
                'AND': [
                    post_query_filter,
                    {'reply_root': None},
                    {'indexed_at': {'lt': cursor_starttime, 'gt': cursor_starttime - timedelta(days=2)}},
                ]
            },
        )

        def score(p: Post):
            # hacker news ranking algo, same as furryli.st is currently utilising
            gravity = 1.85
            time_offset = timedelta(hours=2)
            denom: float = ((cursor_starttime - p.indexed_at + time_offset).total_seconds() / 3600.0) ** gravity
            # also give a flat boost to like count to actually help newer posts get off the ground ?
            return (5 + p.like_count) / denom

        posts.sort(key=score, reverse=True)

        next_up = [i for i in posts if score(i) < lowest_score][:limit]

        cursor = f'{int(cursor_starttime.timestamp() * 1000)}::{min(map(score, next_up))}'
        feed: List[FeedItem] = [{'post': post.uri} for post in next_up]

        return {'cursor': cursor, 'feed': feed}

    return handler


fox_feed = algorithmic_feed({'author': {'is': {'in_fox_feed': {'equals': True}}}})
vix_feed = algorithmic_feed({'author': {'is': {'in_vix_feed': {'equals': True}}}})
