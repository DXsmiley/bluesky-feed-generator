import asyncio
from atproto import AsyncClient
import server.database
from server.database import make_database_connection, Database
from server.load_known_furries import HANDLE, PASSWORD, get_actor_likes, get_likes, store_user, store_like
# from server.gender import guess_gender_reductive
from typing import Set, Tuple, Literal
from server import gender


def guess_gender_reductive(s: str) -> Literal['girl', 'not-girl']:
    vibes = gender.vibecheck(s)
    return 'girl' if vibes.fem and not vibes.masc else 'not-girl'


async def main() -> None:
    db = await make_database_connection()
    client = AsyncClient()
    seen: Set[str] = set()
    await client.login(HANDLE, PASSWORD)
    async for post in get_actor_likes(client, client.me.did):
        if await db.actor.find_first(where={'did': post.post.author.did, 'AND': [server.database.user_is_in_fox_feed]}) is not None:
            async for like in get_likes(client, post.post.uri):
                if like.actor.did not in seen:
                    seen.add(like.actor.did)
                    gender = guess_gender_reductive(like.actor.description or '')
                    if gender == 'girl':
                        if await db.actor.find_unique(where={'did': like.actor.did}) is None:
                            print(like.actor.handle, '-', like.actor.description, '\n')
                            await store_user(db, like.actor)


async def from_likes_of_post(db: Database, post_uri: str) -> Tuple[int, int]:
    added_users = 0
    added_likes = 0
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)
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
