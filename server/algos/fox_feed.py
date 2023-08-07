from datetime import datetime
from typing import Optional, List, Callable

from server.database import Post

from typing_extensions import TypedDict

from prisma.types import PostWhereInput

class FeedItem(TypedDict):
    post: str # ???


class HandlerResult(TypedDict):
    cursor: Optional[str]
    feed: List[FeedItem]

def feed(post_query_filter: PostWhereInput) -> Callable[[Optional[str], int], HandlerResult]:

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

fox_feed = feed({'author': {'is': {'in_fox_feed': {'equals': True}}}})
vix_feed = feed({'author': {'is': {'in_vix_feed': {'equals': True}}}})
