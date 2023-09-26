import asyncio
from datetime import datetime

import foxfeed.monkeypatch

from foxfeed.database import Database

from foxfeed.bsky import (
    AsyncClient,
    get_followers,
    get_follows,
    get_feeds,
    get_likes,
    get_mute_lists,
    get_mutes,
    get_list,
    get_specific_profiles,
    as_detailed_profiles,
)

import foxfeed.bsky

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
)
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView
from atproto.xrpc_client.models.app.bsky.feed.get_likes import Like

import gzip
import json
import traceback
from termcolor import cprint
from dataclasses import dataclass


import foxfeed.algos.generators
from foxfeed.util import parse_datetime, sleep_on, join_unless

from foxfeed.store import store_like, store_post, store_user


async def get_mutuals(client: AsyncClient, did: str, stop_event: Optional[asyncio.Event]) -> AsyncIterable[ProfileView]:
    following_dids = {i.did async for i in get_follows(client, did, stop_event)}
    if stop_event is not None and stop_event.is_set():
        return
    async for i in get_followers(client, did, stop_event):
        if i.did in following_dids:
            yield i


get_people_who_like_the_feed = get_likes


async def get_people_who_like_your_feeds(
    client: AsyncClient, did: str, stop_event: Optional[asyncio.Event]
) -> AsyncIterable[ProfileView]:
    seen: Set[str] = set()
    async for feed in get_feeds(client, did, stop_event):
        async for like in get_people_who_like_the_feed(client, feed.uri, stop_event):
            user = like.actor
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
) -> AsyncIterable[FeedViewPost]:
    async for i in foxfeed.bsky.get_posts(client, did):
        is_repost, indexed_at = (
            (True, i.reason.indexed_at)
            if isinstance(i.reason, ReasonRepost)
            else (False, i.post.indexed_at)
        )
        if after is not None and parse_datetime(indexed_at) < after:
            return
        if include_reposts or not is_repost:
            yield i


async def _get_all_mutes(
    client: AsyncClient,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    # Direct, manual mutes
    async for i in get_mutes(client):
        yield (None, i)
    # Mutes from a mute list
    async for lst in get_mute_lists(client):
        async for i in get_list(client, lst.uri):
            yield (lst, i.subject)


async def get_all_mutes(
    client: AsyncClient,
    *,
    shutdown_event: asyncio.Event,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    async for i, j in _get_all_mutes(client):
        # print('>', j.handle, j.display_name)
        yield (i, j)
        if shutdown_event.is_set():
            break


async def no_connections(_client: AsyncClient, _did: str) -> AsyncIterable[ProfileView]:
    return
    yield


KNOWN_FURRIES_AND_CONNECTIONS = List[
    Tuple[Callable[[AsyncClient, str, Optional[asyncio.Event]], AsyncIterable[ProfileView]], str]
]


@dataclass
class StoreUser:
    user: ProfileViewDetailed
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


async def store_to_db_task(
    shutdown_event: asyncio.Event, db: Database, q: "asyncio.Queue[StoreThing]"
):
    while not shutdown_event.is_set():
        await asyncio.sleep(0.001)
        item = await q.get()
        try:
            if isinstance(item, StoreUser):
                await store_user(
                    db,
                    item.user,
                    is_muted=item.is_muted,
                    is_furrylist_verified=item.is_furrlist_verified,
                    flag_for_manual_review=False,
                )
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
    shutdown_event: asyncio.Event,
    client: AsyncClient,
    only_posts_after: datetime,
    input_queue: "asyncio.Queue[ProfileViewDetailed]",
    llq: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
    output_queue: "asyncio.Queue[StoreThing]",
    *,
    actually_do_shit: bool = True,
):
    cprint("Grabbing posts for furries...", "blue", force_color=True)
    unique = 0
    while not shutdown_event.is_set():
        user = await input_queue.get()
        try:
            if actually_do_shit:
                # cprint(f'Getting posts for {user.handle}', 'blue', force_color=True)
                async for post in get_posts(client, user.did, after=only_posts_after):
                    if post.reply is None:
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
            await sleep_on(shutdown_event, 60)
        finally:
            input_queue.task_done()


async def load_likes_task(
    shutdown_event: asyncio.Event,
    client: AsyncClient,
    input_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
    output_queue: "asyncio.Queue[StoreThing]",
    *,
    actually_do_shit: bool = True,
):
    cprint("Grabbing likes for posts...", "blue", force_color=True)
    while not shutdown_event.is_set():
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
            await sleep_on(shutdown_event, 60)
        finally:
            input_queue.task_done()


async def log_queue_size_task(
    shutdown_event: asyncio.Event,
    storage_queue: "asyncio.Queue[StoreThing]",
    post_load_queue: "asyncio.Queue[ProfileViewDetailed]",
    like_load_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]",
) -> None:
    while not shutdown_event.is_set():
        print(
            f"> Load: {post_load_queue.qsize()} . Like: {like_load_queue.qsize()} . Store: {storage_queue.qsize()}"
        )
        await sleep_on(shutdown_event, 30)


async def find_furries_raw(
    client: AsyncClient, *, shutdown_event: Optional[asyncio.Event]
) -> AsyncIterable[Tuple[ProfileViewDetailed, bool]]:
    known_furries: KNOWN_FURRIES_AND_CONNECTIONS = [
        (get_people_who_like_your_feeds, "puppyfox.bsky.social"),
        (get_follows, "puppyfox.bsky.social"),
        (get_mutuals, "100racs.bsky.social"),
        (get_mutuals, "glitzyfox.bsky.social"),
        (get_mutuals, "itswolven.bsky.social"),
        (get_mutuals, "coolkoinu.bsky.social"),
        (get_mutuals, "gutterbunny.bsky.social"),
        (get_mutuals, "zoeydogy.bsky.social"),
    ]

    cprint("Loading furries from furrtli.st", "blue", force_color=True)
    furrylist = await client.app.bsky.actor.get_profile({"actor": "furryli.st"})
    yield (furrylist, True)
    async for other in as_detailed_profiles(client, get_follows, furrylist.did, shutdown_event):
        yield (other, True)

    cprint("Loading furries from seed list", "blue", force_color=True)
    with gzip.open("./seed.json.gzip", "rb") as sff:
        seed_list = json.loads(sff.read().decode("utf-8"))["seed"]

    async for profile in get_specific_profiles(client, seed_list, shutdown_event):
        yield (profile, False)

    cprint("Grabbing furry-adjacent accounts", "blue", force_color=True)

    for get_associations, handle in known_furries:
        profile = await client.app.bsky.actor.get_profile({"actor": handle})
        yield (profile, False)
        async for other in as_detailed_profiles(client, get_associations, profile.did, shutdown_event):
            yield (other, False)


async def find_furries_clean(
    client: AsyncClient,
    *,
    shutdown_event: Optional[asyncio.Event],
) -> AsyncIterable[Tuple[ProfileViewDetailed, bool]]:
    # Ok so we *know* that the furrylist verified ones are coming out first and we can exploit this to not miss anything
    seen: Set[str] = set()
    async for profile, is_furrylist_verified in find_furries_raw(
        client, shutdown_event=shutdown_event
    ):
        if profile.did not in seen and profile.did:
            seen.add(profile.did)
            yield (profile, is_furrylist_verified)


async def load(
    shutdown_event: asyncio.Event,
    db: Database,
    client: AsyncClient,
    load_posts: bool = True,
    load_likes: bool = True,
) -> None:
    only_posts_after = datetime.now() - foxfeed.algos.generators.LOOKBACK_HARD_LIMIT

    cprint("Getting muted accounts", "blue", force_color=True)
    mutes = {
        i.did async for _, i in get_all_mutes(client, shutdown_event=shutdown_event)
    } - {"" if client.me is None else client.me.did}

    if shutdown_event.is_set():
        return

    queue_size_limit = 20_000

    storage_queue: "asyncio.Queue[StoreThing]" = asyncio.Queue(maxsize=queue_size_limit)
    post_load_queue: "asyncio.Queue[ProfileViewDetailed]" = asyncio.Queue(
        maxsize=queue_size_limit
    )
    like_load_queue: "asyncio.PriorityQueue[Tuple[int, int, FeedViewPost]]" = (
        asyncio.PriorityQueue(maxsize=queue_size_limit)
    )

    storage_worker = asyncio.create_task(
        store_to_db_task(shutdown_event, db, storage_queue)
    )
    load_posts_worker = asyncio.create_task(
        load_posts_task(
            shutdown_event,
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
            shutdown_event,
            client,
            like_load_queue,
            storage_queue,
            actually_do_shit=load_likes,
        )
    )
    report_task = asyncio.create_task(
        log_queue_size_task(
            shutdown_event, storage_queue, post_load_queue, like_load_queue
        )
    )

    async for furry, is_furrlist_verified in find_furries_clean(
        client, shutdown_event=shutdown_event
    ):
        # The posts for a user can be loaded before the user is stored, however the StoreUser will always be ahead of the relevant
        # StorePosts, so this will never break the DB foreign keys
        muted = furry.did in mutes
        await storage_queue.put(StoreUser(furry, is_furrlist_verified, muted))
        if not muted:
            await post_load_queue.put(furry)

    cprint("Waiting for workers to finish...", "blue", force_color=True)

    await join_unless(post_load_queue, shutdown_event)
    await join_unless(storage_queue, shutdown_event)
    await join_unless(like_load_queue, shutdown_event)

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
    if shutdown_event.is_set():
        cprint(
            "Scraping website concluded early due to shutdown signal",
            "green",
            force_color=True,
        )
    else:
        cprint("Done scraping website :)", "green", force_color=True)


async def scan_once(
    shutdown_event: asyncio.Event, db: Database, client: AsyncClient
) -> None:
    cprint("Loading list of furries", "blue", force_color=True)
    mutes = {
        i.did async for _, i in get_all_mutes(client, shutdown_event=shutdown_event)
    } - {"" if client.me is None else client.me.did}
    all_furries = [
        i async for i in find_furries_clean(client, shutdown_event=shutdown_event)
    ]
    cprint("Storing furries", "blue", force_color=True)
    for user, verified in all_furries:
        if shutdown_event.is_set():
            return
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


async def rescan_furry_accounts(
    shutdown_event: asyncio.Event, db: Database, client: AsyncClient, forever: bool
):
    # Do this ONCE
    await load(shutdown_event, db, client)
    while forever and not shutdown_event.is_set():
        try:
            await scan_once(shutdown_event, db, client)
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(f"error while scanning furries", color="red", force_color=True)
            traceback.print_exc()
        await sleep_on(shutdown_event, 60 * 30)
