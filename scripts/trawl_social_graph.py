import asyncio
from asyncio import Queue
from atproto import AsyncClient
from foxfeed.load_known_furries import (
    get_followers,
    get_follows,
    store_to_db,
    StoreUser,
    StorePost,
    HANDLE,
    PASSWORD,
    ProfileView,
)
from foxfeed.gender import guess_gender_reductive
from foxfeed.database import make_database_connection

from typing import Set, List, Union


async def main():
    start = [
        "siggimagenta.bsky.social",
        "jamievx.com",
        "taggzzz.bsky.social",
        "zilchfox.com",
        "mintealon.bsky.social",
        "coffeefoxo.bsky.social",
        "purple-slutsky.com",
        "nubtufts.tuft.party",
        "furryli.st",
        "puppyfox.bsky.social",
    ]
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)
    seen: Set[str] = set(start)
    q: "Queue[Union[StoreUser, StorePost]]" = Queue()
    l: List[str] = list(start)

    db = await make_database_connection()
    worker = asyncio.create_task(store_to_db(set(), db, q))

    async def enq(u: ProfileView):
        d = (u.description or "").lower()
        gender = guess_gender_reductive(d)
        if "furry" in d or "fursuiter" in d or "fursuit maker" in d or "fursuits" in d:
            if u.did not in seen:
                print(gender, u.handle, u.displayName)
                seen.add(u.did)
                l.append(u.did)
                await q.put(StoreUser(u))

    while l:
        c = l.pop()
        async for i in get_followers(client, c):
            await enq(i)
        async for i in get_follows(client, c):
            await enq(i)

    print("Waiting for db worker to finnish...")

    await q.join()
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)


asyncio.run(main())
