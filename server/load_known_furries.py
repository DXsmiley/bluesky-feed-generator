import asyncio
from datetime import datetime

import server.monkeypatch

from server.database import Database, make_database_connection

from atproto import AsyncClient

from publish_feed import HANDLE, PASSWORD

from typing import (
    AsyncIterable,
    Optional,
    Tuple,
    List,
    Callable,
    Set,
    Union,
)

from atproto.xrpc_client.models.app.bsky.actor.defs import (
    ProfileView,
    ProfileViewDetailed,
)
from atproto.xrpc_client.models.app.bsky.feed.defs import (
    FeedViewPost,
    ReasonRepost,
    GeneratorView,
)
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView
from atproto.xrpc_client.models.app.bsky.feed.get_likes import Like
from atproto.xrpc_client.models.app.bsky.embed import images

import gzip
import json
import traceback
from termcolor import cprint
from dataclasses import dataclass
import random
import prisma.errors


import server.algos.fox_feed
import server.algos.score_task
from server.util import mentions_fursuit, parse_datetime
from server import gender


# TODO: Eeeeeeeh
SCORE_REQUIREMENT = server.algos.score_task._raw_score(
    server.algos.score_task.SCORING_CURVE_INFLECTION_POINT, 0
)


def simplify_profile_view(p: ProfileViewDetailed) -> ProfileView:
    return ProfileView(
        did=p.did,
        handle=p.handle,
        avatar=p.avatar,
        description=p.description,
        displayName=p.display_name,
        indexedAt=p.indexed_at,
        labels=p.labels,
        viewer=p.viewer,
    )


async def get_followers(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    r = await client.app.bsky.graph.get_followers({"actor": did})
    for i in r.followers:
        yield i
    while r.cursor:
        r = await client.app.bsky.graph.get_followers({"actor": did, "cursor": r.cursor})
        for i in r.followers:
            yield i


async def get_follows(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    r = await client.app.bsky.graph.get_follows({"actor": did})
    for i in r.follows:
        yield i
    while r.cursor:
        r = await client.app.bsky.graph.get_follows({"actor": did, "cursor": r.cursor})
        for i in r.follows:
            yield i


async def get_mutuals(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    following_dids = {i.did async for i in get_follows(client, did)}
    async for i in get_followers(client, did):
        if i.did in following_dids:
            yield i


async def get_feeds(client: AsyncClient, did: str) -> AsyncIterable[GeneratorView]:
    r = await client.app.bsky.feed.get_actor_feeds({"actor": did})
    for i in r.feeds:
        yield i
    while r.cursor is not None:
        r = await client.app.bsky.feed.get_actor_feeds({"actor": did, "cursor": r.cursor})
        for i in r.feeds:
            yield i


async def get_people_who_like_the_feed(
    client: AsyncClient, uri: str
) -> AsyncIterable[ProfileView]:
    r = await client.app.bsky.feed.get_likes({"uri": uri})
    for i in r.likes:
        yield i.actor
    while r.cursor is not None:
        r = await client.app.bsky.feed.get_likes({"uri": uri, "cursor": r.cursor})
        for i in r.likes:
            yield i.actor


async def get_people_who_like_your_feeds(
    client: AsyncClient, did: str
) -> AsyncIterable[ProfileView]:
    seen: Set[str] = set()
    async for feed in get_feeds(client, did):
        async for user in get_people_who_like_the_feed(client, feed.uri):
            if user.did not in seen:
                seen.add(user.did)
                # print(user.handle, user.display_name)
                yield user


async def get_posts(
    client: AsyncClient,
    did: str,
    *,
    after: Optional[datetime] = None,
    include_reposts: bool = False,
    return_data_if_we_have_it_anyway: bool = False,
) -> AsyncIterable[FeedViewPost]:
    r = None
    while r is None or r.cursor:
        r = await client.app.bsky.feed.get_author_feed(
            {"actor": did, "cursor": r and r.cursor}
        )
        for i in r.feed:
            is_repost, indexed_at = (
                (True, i.reason.indexed_at)
                if isinstance(i.reason, ReasonRepost)
                else (False, i.post.indexed_at)
            )
            if after is not None and parse_datetime(indexed_at) < after:
                r.cursor = None
                if not return_data_if_we_have_it_anyway:
                    return
            if include_reposts or not is_repost:
                yield i


async def get_likes(client: AsyncClient, uri: str) -> AsyncIterable[Like]:
    r = await client.app.bsky.feed.get_likes({"uri": uri})
    for i in r.likes:
        yield i
    while r.cursor is not None:
        r = await client.app.bsky.feed.get_likes({"uri": uri, "cursor": r.cursor})
        for i in r.likes:
            yield i


async def get_actor_likes(
    client: AsyncClient, actor: str
) -> AsyncIterable[FeedViewPost]:
    r = await client.app.bsky.feed.get_actor_likes({"actor": actor})
    for i in r.feed:
        yield i
    while r.cursor is not None:
        r = await client.app.bsky.feed.get_actor_likes({"actor": actor, "cursor": r.cursor})
        for i in r.feed:
            yield i


async def get_mute_lists(client: AsyncClient) -> AsyncIterable[ListView]:
    r = await client.app.bsky.graph.get_list_mutes({})
    for i in r.lists:
        yield i
    while r.cursor:
        r = await client.app.bsky.graph.get_list_mutes({"cursor": r.cursor})
        for i in r.lists:
            yield i


async def _get_all_mutes(
    client: AsyncClient,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    # Direct, manual mutes
    r = await client.app.bsky.graph.get_mutes()
    for i in r.mutes:
        yield (None, i)
    while r.cursor is not None:
        r = await client.app.bsky.graph.get_mutes({"cursor": r.cursor})
        for i in r.mutes:
            yield (None, i)
    # Mutes from a mute list
    async for lst in get_mute_lists(client):
        r = await client.app.bsky.graph.get_list({"list": lst.uri})
        for i in r.items:
            yield (lst, i.subject)
        while r.cursor:
            r = await client.app.bsky.graph.get_list({"list": lst.uri, "cursor": r.cursor})
            for i in r.items:
                yield (lst, i.subject)


async def get_all_mutes(
    client: AsyncClient,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    async for i, j in _get_all_mutes(client):
        # print('>', j.handle, j.display_name)
        yield (i, j)


async def get_many_profiles(
    client: AsyncClient, dids: List[str]
) -> AsyncIterable[ProfileView]:
    for i in range(0, len(dids), 25):
        r = await client.app.bsky.actor.get_profiles({"actors": dids[i : i + 25]})
        for p in r.profiles:
            yield simplify_profile_view(p)


async def no_connections(_client: AsyncClient, _did: str) -> AsyncIterable[ProfileView]:
    return
    yield


KNOWN_FURRIES_AND_CONNECTIONS = List[
    Tuple[Callable[[AsyncClient, str], AsyncIterable[ProfileView]], str]
]


@dataclass
class StoreUser:
    user: ProfileView
    is_furrlist_verified: bool
    is_muted: bool


@dataclass
class StorePost:
    post: FeedViewPost


@dataclass
class StoreLike:
    post_uri: str
    like: Like


StoreThing = Union[StoreUser, StorePost, StoreLike]


async def store_user(
    db: Database,
    user: ProfileView,
    *,
    is_muted: bool,
    is_furrylist_verified: bool,
    flag_for_manual_review: bool,
) -> None:
    gender_vibes = gender.vibecheck(user.description or "")
    await db.actor.upsert(
        where={"did": user.did},
        data={
            "create": {
                "did": user.did,
                "handle": user.handle,
                "description": user.description,
                "displayName": user.display_name,
                "avatar": user.avatar,
                "flagged_for_manual_review": flag_for_manual_review,
                "autolabel_fem_vibes": gender_vibes.fem,
                "autolabel_nb_vibes": gender_vibes.enby,
                "autolabel_masc_vibes": gender_vibes.masc,
                "is_furrylist_verified": is_furrylist_verified,  # TODO
                "is_muted": is_muted,
            },
            "update": {
                "did": user.did,
                "handle": user.handle,
                "description": user.description,
                "displayName": user.display_name,
                "avatar": user.avatar,
                "autolabel_fem_vibes": gender_vibes.fem,
                "autolabel_nb_vibes": gender_vibes.enby,
                "autolabel_masc_vibes": gender_vibes.masc,
                "is_muted": is_muted,
                "is_furrylist_verified": is_furrylist_verified,
                # 'flagged_for_manual_review': flag_for_manual_review,
            },
        },
    )


async def store_like(
    db: Database, post_uri: str, like: Like
) -> Optional[prisma.models.Like]:
    ugh = datetime.utcnow().isoformat()
    blh = random.randint(0, 1 << 32)
    uri = f"fuck://{ugh}-{blh}"
    try:
        return await db.like.create(
            data={
                "uri": uri,  # TODO
                "cid": "",  # TODO
                "post_uri": post_uri,
                "post_cid": "",  # TODO
                "liker_id": like.actor.did,
                "created_at": parse_datetime(like.created_at),
            }
        )
    except prisma.errors.UniqueViolationError:
        pass
    except prisma.errors.ForeignKeyViolationError:
        pass
    return None


async def store_post(db: Database, post: FeedViewPost) -> None:
    p = post.post
    reply_parent = None if post.reply is None else post.reply.parent.uri
    reply_root = None if post.reply is None else post.reply.root.uri
    media = p.embed.images if isinstance(p.embed, images.View) else []
    media_with_alt_text = sum(i.alt != "" for i in media)
    # if verbose:
    #     print(f'- ({p.uri}, {media_count} images, {p.likeCount or 0} likes) - {p.record["text"]}')
    create: prisma.types.PostCreateInput = {
        "uri": p.uri,
        "cid": p.cid,
        # TODO: Fix these
        "reply_parent": reply_parent,
        "reply_root": reply_root,
        "indexed_at": parse_datetime(p.indexed_at),
        "like_count": p.like_count or 0,
        "authorId": p.author.did,
        "mentions_fursuit": mentions_fursuit(p.record.text),
        "media_count": len(media),
        "media_with_alt_text_count": media_with_alt_text,
        "text": p.record.text,
        "m0": None if len(media) <= 0 else media[0].thumb,
        "m1": None if len(media) <= 1 else media[1].thumb,
        "m2": None if len(media) <= 2 else media[2].thumb,
        "m3": None if len(media) <= 3 else media[3].thumb,
    }
    update: prisma.types.PostUpdateInput = {
        "like_count": p.like_count or 0,
        "media_count": len(media),
        "media_with_alt_text_count": media_with_alt_text,
        "mentions_fursuit": mentions_fursuit(p.record.text),
        "text": p.record.text,
        "m0": None if len(media) <= 0 else media[0].thumb,
        "m1": None if len(media) <= 1 else media[1].thumb,
        "m2": None if len(media) <= 2 else media[2].thumb,
        "m3": None if len(media) <= 3 else media[3].thumb,
    }
    await db.post.upsert(
        where={"uri": p.uri},
        data={
            "create": create,
            "update": update,
        },
    )


async def store_to_db_task(db: Database, q: "asyncio.Queue[StoreThing]"):
    while True:
        await asyncio.sleep(0.001)
        item = await q.get()
        try:
            if isinstance(item, StoreUser):
                await store_user(db, item.user, is_muted=item.is_muted, is_furrylist_verified=item.is_furrlist_verified, flag_for_manual_review=False)
            elif isinstance(item, StorePost):
                await store_post(db, item.post)
            elif isinstance(item, StoreLike):
                await store_like(db, item.post_uri, item.like)
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(f"Error during handling item: {item}", color="red", force_color=True)
            traceback.print_exc()
            await asyncio.sleep(1)
        finally:
            q.task_done()


async def load_posts_task(
    client: AsyncClient,
    only_posts_after: datetime,
    input_queue: "asyncio.Queue[ProfileView]",
    llq: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
    output_queue: "asyncio.Queue[StoreThing]",
    *,
    actually_do_shit: bool = True,
):
    cprint("Grabbing posts for furries...", "blue", force_color=True)
    unique = 0
    while True:
        user = await input_queue.get()
        try:
            if actually_do_shit:
                # cprint(f'Getting posts for {user.handle}', 'blue', force_color=True)
                async for post in get_posts(client, user.did, after=only_posts_after):
                    score = server.algos.score_task._raw_score(
                        datetime.now() - parse_datetime(post.post.indexed_at),
                        post.post.like_count or 0,
                    )
                    if post.reply is None and score > SCORE_REQUIREMENT:
                        await output_queue.put(StorePost(post))
                        await llq.put((-(post.post.like_count or 0), unique, post))
                        # hack to prevent post objects being compared against each other
                        unique += 1
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(
                f"error while getting posts for user {user.handle}",
                color="red",
                force_color=True,
            )
            traceback.print_exc()
            await asyncio.sleep(60)
        finally:
            input_queue.task_done()


async def load_likes_task(
    client: AsyncClient,
    input_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
    output_queue: "asyncio.Queue[StoreThing]",
    *,
    actually_do_shit: bool = True,
):
    cprint("Grabbing likes for posts...", "blue", force_color=True)
    while True:
        _, _, post = await input_queue.get()
        try:
            if actually_do_shit:
                async for like in get_likes(client, post.post.uri):
                    await output_queue.put(StoreLike(post.post.uri, like))
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(
                f"error while getting likes for post {post.post.uri}",
                color="red",
                force_color=True,
            )
            traceback.print_exc()
            await asyncio.sleep(60)
        finally:
            input_queue.task_done()


async def log_queue_size_task(
    storage_queue: "asyncio.Queue[StoreThing]",
    post_load_queue: "asyncio.Queue[ProfileView]",
    like_load_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
) -> None:
    while True:
        print(
            f"> Load: {post_load_queue.qsize()} . Like: {like_load_queue.qsize()} . Store: {storage_queue.qsize()}"
        )
        await asyncio.sleep(30)


async def find_furries_raw(
    client: AsyncClient,
) -> AsyncIterable[Tuple[ProfileView, bool]]:
    known_furries: KNOWN_FURRIES_AND_CONNECTIONS = [
        (get_people_who_like_your_feeds, "puppyfox.bsky.social"),
        (get_follows, "puppyfox.bsky.social"),
        (get_follows, "furryli.st"),
        (get_mutuals, "100racs.bsky.social"),
        (get_mutuals, "glitzyfox.bsky.social"),
        (get_mutuals, "itswolven.bsky.social"),
        (get_mutuals, "coolkoinu.bsky.social"),
        (get_mutuals, "gutterbunny.bsky.social"),
        (get_mutuals, "zoeydogy.bsky.social"),
    ]

    cprint("Loading furries from furrtli.st", "blue", force_color=True)
    furrylist = simplify_profile_view(
        await client.app.bsky.actor.get_profile({"actor": "furryli.st"})
    )
    yield (furrylist, True)
    async for other in get_follows(client, furrylist.did):
        yield (other, True)

    cprint("Loading furries from seed list", "blue", force_color=True)
    with gzip.open("./seed.json.gzip", "rb") as sff:
        seed_list = json.loads(sff.read().decode("utf-8"))["seed"]

    async for profile in get_many_profiles(client, seed_list):
        yield (profile, False)

    cprint("Grabbing furry-adjacent accounts", "blue", force_color=True)

    for get_associations, handle in known_furries:
        profile = simplify_profile_view(
            await client.app.bsky.actor.get_profile({"actor": handle})
        )
        yield (profile, False)
        async for other in get_associations(client, profile.did):
            yield (other, False)


async def find_furries_clean(
    client: AsyncClient,
) -> AsyncIterable[Tuple[ProfileView, bool]]:
    # Ok so we *know* that the furrylist verified ones are coming out first and we can exploit this to not miss anything
    seen: Set[str] = set()
    async for profile, is_furrylist_verified in find_furries_raw(client):
        if profile.did not in seen and profile.did:
            seen.add(profile.did)
            yield (profile, is_furrylist_verified)


async def load(db: Database, client: AsyncClient, load_posts: bool = True, load_likes: bool = True) -> None:
    only_posts_after = datetime.now() - server.algos.score_task.LOOKBACK_HARD_LIMIT

    cprint("Getting muted accounts", "blue", force_color=True)
    mutes = {i.did async for _, i in get_all_mutes(client)} - {
        "" if client.me is None else client.me.did
    }

    queue_size_limit = 20_000

    storage_queue: "asyncio.Queue[StoreThing]" = asyncio.Queue(maxsize=queue_size_limit)
    post_load_queue: "asyncio.Queue[ProfileView]" = asyncio.Queue(
        maxsize=queue_size_limit
    )
    like_load_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]" = (
        asyncio.PriorityQueue(maxsize=queue_size_limit)
    )

    storage_worker = asyncio.create_task(store_to_db_task(db, storage_queue))
    load_posts_worker = asyncio.create_task(
        load_posts_task(
            client,
            only_posts_after,
            post_load_queue,
            like_load_queue,
            storage_queue,
            actually_do_shit=load_posts,
        )
    )
    load_likes_worker = asyncio.create_task(
        load_likes_task(
            client, like_load_queue, storage_queue, actually_do_shit=load_likes
        )
    )
    report_task = asyncio.create_task(
        log_queue_size_task(storage_queue, post_load_queue, like_load_queue)
    )

    async for furry, is_furrlist_verified in find_furries_clean(client):
        # The posts for a user can be loaded before the user is stored, however the StoreUser will always be ahead of the relevant
        # StorePosts, so this will never break the DB foreign keys
        muted = furry.did in mutes
        await storage_queue.put(StoreUser(furry, is_furrlist_verified, muted))
        if not muted:
            await post_load_queue.put(furry)

    cprint("Waiting for workers to finish...", "blue", force_color=True)

    await post_load_queue.join()
    await storage_queue.join()
    await like_load_queue.join()

    storage_worker.cancel()
    load_posts_worker.cancel()
    load_likes_worker.cancel()
    report_task.cancel()

    await asyncio.gather(
        storage_worker,
        load_posts_worker,
        load_likes_worker,
        report_task,
        return_exceptions=True,
    )

    cprint("Ok! Yeah! Woooo!", "blue", force_color=True)
    cprint("Done scraping website :)", "green", force_color=True)


async def scan_once(db: Database, client: AsyncClient):
    cprint("Loading list of furries", "blue", force_color=True)
    mutes = {i.did async for _, i in get_all_mutes(client)} - {
        "" if client.me is None else client.me.did
    }
    all_furries = [i async for i in find_furries_clean(client)]
    cprint("Storing furries", "blue", force_color=True)
    for user, verified in all_furries:
        await store_user(
            db,
            user,
            is_furrylist_verified=verified,
            flag_for_manual_review=False,
            is_muted=(user.did in mutes),
        )
    # some accounts may have previously been in the dataset but are now excluded
    await db.actor.update_many(
        where={"did": {"in": list(mutes)}},
        data={"is_muted": True},
    )
    cprint("Done", "blue", force_color=True)


async def rescan_furry_accounts_forever(db: Database):
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)
    # Do this ONCE
    await load(db, client)
    while True:
        try:
            await scan_once(db, client)
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(f"error while scanning furries", color="red", force_color=True)
            traceback.print_exc()
        await asyncio.sleep(60 * 30)


async def main():
    db = await make_database_connection()
    await load(db, load_posts=True)


if __name__ == "__main__":
    asyncio.run(main())
