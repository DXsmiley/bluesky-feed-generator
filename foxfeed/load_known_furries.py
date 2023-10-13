import asyncio
from datetime import datetime, timedelta

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
    get_specific_posts,
    get_specific_likes,
    as_detailed_profiles,
    PostView
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

import prisma.errors

import foxfeed.algos.generators
from foxfeed.gen.db import find_unlinks
from foxfeed.util import parse_datetime, sleep_on, join_unless
from foxfeed.store import store_like, store_post, store_post3, store_user
from foxfeed.res import Res


# TODO: Remove this somehow
import foxfeed.data_filter


async def get_mutuals(client: AsyncClient, did: str, policy: foxfeed.bsky.PolicyType) -> AsyncIterable[ProfileView]:
    following_dids = {i.did async for i in get_follows(client, did, policy)}
    async for i in get_followers(client, did, policy):
        if i.did in following_dids:
            yield i


get_people_who_like_the_feed = get_likes


async def get_people_who_like_your_feeds(
    client: AsyncClient, did: str, policy: foxfeed.bsky.PolicyType
) -> AsyncIterable[ProfileView]:
    seen: Set[str] = set()
    async for feed in get_feeds(client, did, policy):
        async for like in get_people_who_like_the_feed(client, feed.uri, policy):
            user = like.actor
            if user.did not in seen:
                seen.add(user.did)
                # print(user.handle, user.display_name)
                yield user


async def get_posts(
    client: AsyncClient,
    did: str,
    *,
    policy: foxfeed.bsky.Policy,
    after: Optional[datetime] = None,
    include_reposts: bool = False,
) -> AsyncIterable[FeedViewPost]:
    async for i in foxfeed.bsky.get_posts(client, did, policy):
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
    policy: foxfeed.bsky.Policy,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    # Direct, manual mutes
    async for i in get_mutes(client, policy):
        yield (None, i)
    # Mutes from a mute list
    async for lst in get_mute_lists(client, policy):
        async for i in get_list(client, lst.uri, policy):
            yield (lst, i.subject)


async def get_all_mutes(
    client: AsyncClient,
    *,
    policy: foxfeed.bsky.Policy,
) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    async for i, j in _get_all_mutes(client, policy):
        # print('>', j.handle, j.display_name)
        yield (i, j)
        if policy.stop_event.is_set():
            break


async def get_mutes_across_clients(
    clients: List[AsyncClient],
    *,
    policy: foxfeed.bsky.Policy
) -> Set[str]:
    return (
        {i.did for c in clients async for _, i in get_all_mutes(c, policy=policy)}
        - {c.me.did for c in clients if c.me is not None}
    )


async def no_connections(_client: AsyncClient, _did: str) -> AsyncIterable[ProfileView]:
    return
    yield


KNOWN_FURRIES_AND_CONNECTIONS = List[
    Tuple[Callable[[AsyncClient, str, foxfeed.bsky.PolicyType], AsyncIterable[ProfileView]], str]
]


@dataclass
class StoreUser:
    user: ProfileViewDetailed
    is_furrylist_verified: bool
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
                    is_furrylist_verified=item.is_furrylist_verified,
                    flag_for_manual_review=False,
                    is_external_to_network=False,
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
    policy: foxfeed.bsky.Policy,
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
                async for post in get_posts(client, user.did, after=only_posts_after, policy=policy):
                    await output_queue.put(StorePost(post))
                    # Currently care about replies but aren't really fussed about likes on them TBH
                    if post.reply is None:
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
    policy: foxfeed.bsky.Policy,
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
                async for like in get_likes(client, post.post.uri, policy):
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
    client: AsyncClient, *, policy: foxfeed.bsky.Policy,
) -> AsyncIterable[Tuple[ProfileViewDetailed, bool]]:
    known_furries: KNOWN_FURRIES_AND_CONNECTIONS = [
        (get_people_who_like_your_feeds, "puppyfox.bsky.social"),
        (get_people_who_like_your_feeds, "foxfeed.bsky.social"),
        (get_follows, "puppyfox.bsky.social"),
        (get_mutuals, "100racs.bsky.social"),
        (get_mutuals, "glitzyfox.bsky.social"),
        (get_mutuals, "itswolven.bsky.social"),
        (get_mutuals, "coolkoinu.bsky.social"),
        (get_mutuals, "gutterbunny.bsky.social"),
        (get_mutuals, "zoeydogy.bsky.social"),
        (get_mutuals, "meanshep.bsky.social"),
        (get_mutuals, "zempy3.bsky.social"),
        (get_mutuals, "jamievx.com"),
    ]

    cprint("Loading furries from furryli.st", "blue", force_color=True)
    furrylist = await client.app.bsky.actor.get_profile({"actor": "furryli.st"})
    yield (furrylist, True)
    async for other in as_detailed_profiles(client, get_follows, furrylist.did, policy):
        yield (other, True)

    cprint("Loading furries from seed list", "blue", force_color=True)
    with gzip.open("./seed.json.gzip", "rb") as sff:
        seed_list = json.loads(sff.read().decode("utf-8"))["seed"]

    async for profile in get_specific_profiles(client, seed_list, policy):
        yield (profile, False)

    cprint("Grabbing furry-adjacent accounts", "blue", force_color=True)

    for get_associations, handle in known_furries:
        if policy.stop_event.is_set():
            break
        profile = await client.app.bsky.actor.get_profile({"actor": handle})
        yield (profile, False)
        async for other in as_detailed_profiles(client, get_associations, profile.did, policy):
            yield (other, False)


async def find_furries_clean(
    client: AsyncClient,
    *,
    policy: foxfeed.bsky.Policy,
) -> AsyncIterable[Tuple[ProfileViewDetailed, bool]]:
    # Ok so we *know* that the furrylist verified ones are coming out first and we can exploit this to not miss anything
    seen: Set[str] = set()
    async for profile, is_furrylist_verified in find_furries_raw(client, policy=policy):
        if profile.did not in seen and profile.did:
            seen.add(profile.did)
            yield (profile, is_furrylist_verified)


async def load(
    shutdown_event: asyncio.Event,
    db: Database,
    client: AsyncClient,
    personal_bsky_client: AsyncClient,
    policy: foxfeed.bsky.Policy,
    load_posts: bool = True,
    load_likes: bool = True,
) -> None:
    only_posts_after = datetime.now() - foxfeed.algos.generators.LOOKBACK_HARD_LIMIT

    cprint("Getting muted accounts", "blue", force_color=True)
    mutes = await get_mutes_across_clients([client, personal_bsky_client], policy=policy)

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
            policy,
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
            policy,
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
        client, policy=policy
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
    shutdown_event: asyncio.Event,
    db: Database,
    client: AsyncClient,
    personal_bsky_client: AsyncClient,
    policy: foxfeed.bsky.Policy,
) -> None:
    cprint("Loading list of furries", "blue", force_color=True)
    mutes = await get_mutes_across_clients([client, personal_bsky_client], policy=policy)
    all_furries = [
        i async for i in find_furries_clean(client, policy=policy)
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
            is_external_to_network=False,
        )
    # some accounts may have previously been in the dataset but are now excluded
    await db.actor.update_many(
        where={"did": {"in": list(mutes)}},
        data={"is_muted": True},
    )
    cprint("Done", "blue", force_color=True)


async def enqueue_unlinks(db: Database) -> int:
    print('Finding unlinks!')
    unlinks = await find_unlinks(db)
    return await db.unknownthing.create_many(
        data=[{'kind': 'post', 'identifier': i.uri} for i in unlinks],
        skip_duplicates=True
    )


async def create_sentinels(db: Database):
    await db.actor.upsert(
        where={'did': 'unknown'},
        data={
            'create': {
                'did': 'unknown',
                'handle': 'unknown',
                'is_muted': True,
            },
            'update': {
                'did': 'unknown',
                'handle': 'unknown',
                'is_muted': True,
            }
        }
    )


async def load_unknown_things(db: Database, client: AsyncClient, policy: foxfeed.bsky.Policy) -> bool:
    cprint(f"There are {await db.unknownthing.count(where={'kind': {'in': ['actor', 'post', 'like']}})} unknown things", "cyan", force_color=True)

    max_id = await db.unknownthing.find_first(order={'id': 'desc'}, where={'kind': {'in': ['actor', 'post', 'like']}})
    if max_id is None:
        if await enqueue_unlinks(db):
            return True
        cprint("No unknown things to load", "blue", force_color=True)
        return False
    
    block_size = 200

    cprint("Loading unknown users", "blue", force_color=True)
    while x := await db.unknownthing.find_many(
        take=block_size,
        order={'id': 'asc'},
        where={'id': {'lte': max_id.id}, 'kind': 'actor'},
    ):
        users = [i async for i in get_specific_profiles(client, [i.identifier for i in x], policy)]
        # Need to check HERE in order to not process an incomplete set of profiles and then accidentally
        # drop incomplete work from the table. This is bad design, like the fact there's this weird edge case this actually.
        # Might just want to raise an exception from within the bsky queries TBH.
        if policy.stop_event.is_set():
            break
        gone = {i.identifier for i in x} - {i.did for i in users}
        async with db.tx() as tx:
            for did in gone:
                await tx.actor.upsert(
                    where={'did': did},
                    data={
                        'create': {
                            'did': did,
                            'handle': 'deleted',
                            'is_muted': True,
                        },
                        'update': {}
                    }
                )
            for user in users:
                cprint(f'{user.handle} {user.display_name}', 'yellow', force_color=True)
                await store_user(
                    tx,
                    user,
                    is_muted=False,
                    is_furrylist_verified=False,
                    flag_for_manual_review=False,
                    is_external_to_network=True
                )
            await tx.unknownthing.delete_many(where={'id': {'in': [i.id for i in x]}})
        for i in x:
            await foxfeed.data_filter.user_exists_cached.drop_entry(i.identifier)

    cprint("Loading unknown posts", "blue", force_color=True)
    while x := await db.unknownthing.find_many(
        take=25,
        order={'id': 'asc'},
        where={'id': {'lte': max_id.id}, 'kind': 'post'},
    ):
        posts = [i async for i in get_specific_posts(client, [i.identifier for i in x], policy)]
        if policy.stop_event.is_set():
            break
        # query didn't return information about these posts, assume they've been deleted
        gone = {i.identifier for i in x} - {i.uri for i in posts}
        author_dids = [i.author.did for i in posts]
        author_dids_we_have = [
            i.did
            for i in await db.actor.find_many(where={'did': {'in': author_dids}})
        ]
        ready_to_store = [
            i
            for i in posts
            if i.author.did in author_dids_we_have
        ]
        not_ready_to_store = [
            i
            for i in posts
            if i.author.did not in author_dids_we_have
        ]
        async with db.tx() as tx:
            for i in gone:
                await tx.post.upsert(
                    where={'uri': i},
                    data={
                        'create': {
                            'uri': i,
                            'cid': 'unknown',
                            'text': 'unknown',
                            'is_deleted': True,
                            'mentions_fursuit': False,
                            'authorId': 'unknown',
                            'media_count': 0,
                        },
                        'update': {
                            'is_deleted': True
                        }
                    }
                )
            for post in ready_to_store:
                await store_post3(tx, post)
            await tx.unknownthing.delete_many(where={'id': {'in': [i.id for i in x]}})
            if not_ready_to_store:
                await tx.unknownthing.create_many(
                    data=[{'kind': 'actor', 'identifier': i.author.did} for i in not_ready_to_store],
                    skip_duplicates=True
                )
                await tx.unknownthing.create_many(
                    data=[{'kind': 'post', 'identifier': i.uri} for i in not_ready_to_store],
                    skip_duplicates=True
                )
        for i in x:
            await foxfeed.data_filter.post_exists_cached.drop_entry(i.identifier)

    cprint("Loading unknown likes", "blue", force_color=True)
    while x := await db.unknownthing.find_many(
        take=block_size,
        order={'identifier': 'asc'},
        # Low key don't care about the ID limiter here, we care more about getting value from queries TBH
        where={'kind': 'like'},
    ):
        likes = [i async for i in get_specific_likes(client, [i.identifier for i in x], policy)]
        # Need to check HERE in order to not process an incomplete set of profiles and then accidentally
        # drop incomplete work from the table. This is bad design, like the fact there's this weird edge case this actually.
        # Might just want to raise an exception from within the bsky queries TBH.
        if policy.stop_event.is_set():
            break
        print(f'Storing {len(likes)} likes')
        for i in likes:
            try:
                await db.like.create(
                    data={
                        "uri": i.uri,
                        "cid": i.cid or "",
                        "post_uri": i.post_uri,
                        "post_cid": i.post_cid,
                        "liker_id": i.actor_did,
                        "created_at": parse_datetime(i.created_at)
                    }
                )
            except prisma.errors.ForeignKeyViolationError:
                pass
            except prisma.errors.UniqueViolationError:
                pass
        await db.unknownthing.delete_many(where={'id': {'in': [i.id for i in x]}})
        if len(x) < block_size:
            # Need to break like this since we don't have the ID limit
            # Otherwise we risk spinning here forever as the firehose adds new things to the work queue constantly
            break
            # Actually I think we risk never moving on to the main scraper at this rate lmao
    
    return True


async def rescan_furry_accounts(
    res: Res, forever: bool
):
    # Actual rate limit is 3000/5 minutes,
    # however we artifically limit this part of the system
    # https://atproto.com/blog/rate-limits-pds-v3
    policy = foxfeed.bsky.Policy(
        res.shutdown_event,
        timedelta(minutes=5),
        2700
    )
    await create_sentinels(res.db)
    try:
        while await load_unknown_things(res.db, res.client, policy):
            pass
    except asyncio.CancelledError:
        return
    except KeyboardInterrupt:
        return
    except Exception:
        cprint(f"error while loading unknown things", color="red", force_color=True)
        traceback.print_exc()
    await load(res.shutdown_event, res.db, res.client, res.personal_bsky_client, policy)
    while forever and not res.shutdown_event.is_set():
        try:
            while await load_unknown_things(res.db, res.client, policy):
                pass
            await scan_once(res.shutdown_event, res.db, res.client, res.personal_bsky_client, policy)
        except asyncio.CancelledError:
            break
        except KeyboardInterrupt:
            break
        except Exception:
            cprint(f"error while scanning furries", color="red", force_color=True)
            traceback.print_exc()
        await sleep_on(res.shutdown_event, 60 * 30)
