import asyncio
from atproto import AsyncClient
import foxfeed.database
from foxfeed.database import make_database_connection, Database
from foxfeed.bsky import get_actor_likes, get_likes
from foxfeed.store import store_user, store_like
# from server.gender import guess_gender_reductive
from typing import Set, Tuple, Literal
from foxfeed import gender
from foxfeed.bsky import AsyncClient, make_bsky_client


def guess_gender_reductive(s: str) -> Literal['girl', 'not-girl']:
    vibes = gender.vibecheck(s)
    return 'girl' if vibes.fem and not vibes.masc else 'not-girl'


async def main() -> None:
    db = await make_database_connection()
    client = await make_bsky_client(db)
    seen: Set[str] = set()
    async for post in get_actor_likes(client, client.me.did):
        if await db.actor.find_first(where={'did': post.post.author.did, 'AND': [foxfeed.database.user_is_in_fox_feed]}) is not None:
            async for like in get_likes(client, post.post.uri):
                if like.actor.did not in seen:
                    seen.add(like.actor.did)
                    gender = guess_gender_reductive(like.actor.description or '')
                    if gender == 'girl':
                        if await db.actor.find_unique(where={'did': like.actor.did}) is None:
                            print(like.actor.handle, '-', like.actor.description, '\n')
                            await store_user(db, like.actor)


async def from_likes_of_post(db: Database, client: AsyncClient, post_uri: str) -> Tuple[int, int]:
    added_users = 0
    added_likes = 0
    async for like in get_likes(client, post_uri):
        gender = guess_gender_reductive(like.actor.description or '')
        if gender == 'girl' and await db.actor.find_unique(where={'did': like.actor.did}) is None:
            # Can assume that this is a create
            await store_user(db, like.actor, flag_for_manual_review=True, is_furrylist_verified=False, is_muted=False)
            added_users += 1
        if await store_like(db, post_uri, like) is not None:
            added_likes += 1
    return (added_users, added_likes)

if __name__ == '__main__':
    asyncio.run(main())
