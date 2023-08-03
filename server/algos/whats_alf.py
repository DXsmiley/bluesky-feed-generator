from datetime import datetime
from typing import Optional, List

from server import config
from server.database import Post

from typing_extensions import TypedDict

uri = config.WHATS_ALF_URI


class FeedItem(TypedDict):
    post: str # ???


class HandlerResult(TypedDict):
    cursor: Optional[str]
    feed: List[FeedItem]


def handler(cursor: Optional[str], limit: int) -> HandlerResult:
    # posts = Post.select().order_by(Post.indexed_at.desc()).order_by(Post.cid.desc()).limit(limit)

    if cursor:
        cursor_parts = cursor.split('::')
        if len(cursor_parts) != 2:
            raise ValueError('Malformed cursor')

        indexed_at_str, cid = cursor_parts
        indexed_at = datetime.fromtimestamp(int(indexed_at_str) / 1000)
        posts = Post.prisma().find_many(
            take=limit,
            where={
                'AND': [
                    {'indexed_at': {'lt': indexed_at}},
                    {'cid': {'lt': cid}},
                ]
            },
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],

        )
        # posts = posts.where(Post.indexed_at <= indexed_at).where(Post.cid < cid)
    else:
        posts = Post.prisma().find_many(
            take=limit,
            # skip=...,
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
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
