import asyncio
from datetime import datetime, timezone, timedelta

import server.monkeypatch

from server.database import Database, make_database_connection

from atproto import AsyncClient, models

from publish_feed import HANDLE, PASSWORD

from typing import Iterable, AsyncIterable, Optional, Dict, Tuple, List, Callable, Set, Union, Literal

from atproto.xrpc_client.models.app.bsky.actor.defs import ProfileView, ProfileViewDetailed
from atproto.xrpc_client.models.app.bsky.feed.defs import FeedViewPost, ReasonRepost, GeneratorView
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView, ListItemView
from atproto.xrpc_client.models.app.bsky.embed import images

import gzip
import json
import traceback
from termcolor import cprint
from dataclasses import dataclass


import server.algos.fox_feed
import server.algos.score_task
from server.data_filter import mentions_fursuit
from server.gender import guess_gender_reductive


# TODO: Eeeeeeeh
SCORE_REQUIREMENT = server.algos.score_task._raw_score(
    server.algos.score_task.SCORING_CURVE_INFLECTION_POINT,
    0
)


def simplify_profile_view(p: ProfileViewDetailed) -> ProfileView:
    return ProfileView(
        did=p.did,
        handle=p.handle,
        avatar=p.avatar,
        description=p.description,
        displayName=p.displayName,
        indexedAt=p.indexedAt,
        labels=p.labels,
        viewer=p.viewer
    )


async def get_followers(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    r = await client.bsky.graph.get_followers({'actor': did})
    for i in r.followers:
        yield i
    while r.cursor:
        r = await client.bsky.graph.get_followers({'actor': did, 'cursor': r.cursor})
        for i in r.followers:
            yield i


async def get_follows(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    r = await client.bsky.graph.get_follows({'actor': did})
    for i in r.follows:
        yield i
    while r.cursor:
        r = await client.bsky.graph.get_follows({'actor': did, 'cursor': r.cursor})
        for i in r.follows:
            yield i


async def get_mutuals(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    following_dids = {i.did async for i in get_follows(client, did)}
    async for i in get_followers(client, did):
        if i.did in following_dids:
            yield i


async def get_feeds(client: AsyncClient, did: str) -> AsyncIterable[GeneratorView]:
    r = await client.bsky.feed.get_actor_feeds({'actor': did})
    for i in r.feeds:
        yield i
    while r.cursor is not None:
        r = await client.bsky.feed.get_actor_feeds({'actor': did, 'cursor': r.cursor})
        for i in r.feeds:
            yield i


async def get_people_who_like_the_feed(client: AsyncClient, uri: str) -> AsyncIterable[ProfileView]:
    r = await client.bsky.feed.get_likes({'uri': uri})
    for i in r.likes:
        yield i.actor
    while r.cursor is not None:
        r = await client.bsky.feed.get_likes({'uri': uri, 'cursor': r.cursor})
        for i in r.likes:
            yield i.actor


async def get_people_who_like_your_feeds(client: AsyncClient, did: str) -> AsyncIterable[ProfileView]:
    seen: Set[str] = set()
    async for feed in get_feeds(client, did):
        async for user in get_people_who_like_the_feed(client, feed.uri):
            if user.did not in seen:
                seen.add(user.did)
                # print(user.handle, user.displayName)
                yield user


def parse_datetime(s: str) -> datetime:
    formats = [
        r'%Y-%m-%dT%H:%M:%S.%fZ',
        r'%Y-%m-%dT%H:%M:%S.%f',
        r'%Y-%m-%dT%H:%M:%SZ',
        r'%Y-%m-%dT%H:%M:%S',
        r'%Y-%m-%dT%H:%M:%S.%f+00:00',
        r'%Y-%m-%dT%H:%M:%S+00:00',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f'failed to parse datetime string "{s}"')


async def get_posts(
    client: AsyncClient,
    did: str,
    *,
    after: Optional[datetime]=None,
    include_reposts: bool=False,
    return_data_if_we_have_it_anyway: bool=False
) -> AsyncIterable[FeedViewPost]:
    r = None
    while r is None or r.cursor:
        r = await client.bsky.feed.get_author_feed({'actor': did, 'cursor': r and r.cursor})
        for i in r.feed:
            is_repost, indexed_at = (
                (True, i.reason.indexedAt) if isinstance(i.reason, ReasonRepost)
                else (False, i.post.indexedAt)
            )
            if after is not None and parse_datetime(indexed_at) < after:
                r.cursor = None
                if not return_data_if_we_have_it_anyway:
                    return
            if include_reposts or not is_repost:
                yield i


async def get_mute_lists(client: AsyncClient) -> AsyncIterable[ListView]:
    r = await client.bsky.graph.get_list_mutes({})
    for i in r.lists:
        yield i
    while r.cursor:
        r = await client.bsky.graph.get_list_mutes({'cursor': r.cursor})
        for i in r.lists:
            yield i


async def _get_all_mutes(client: AsyncClient) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    # Direct, manual mutes
    r = await client.bsky.graph.get_mutes()
    for i in r.mutes:
        yield (None, i)
    while r.cursor is not None:
        r = await client.bsky.graph.get_mutes({'cursor': r.cursor})
        for i in r.mutes:
            yield (None, i)
    # Mutes from a mute list
    async for lst in get_mute_lists(client):
        r = await client.bsky.graph.get_list({'list': lst.uri})
        for i in r.items:
            yield (lst, i.subject)
        while r.cursor:
            r = await client.bsky.graph.get_list({'list': lst.uri, 'cursor': r.cursor})
            for i in r.items:
                yield (lst, i.subject)


async def get_all_mutes(client: AsyncClient) -> AsyncIterable[Tuple[Optional[ListView], ProfileView]]:
    async for i, j in _get_all_mutes(client):
        # print('>', j.handle, j.displayName)
        yield (i, j)


async def get_many_profiles(client: AsyncClient, dids: List[str]) -> AsyncIterable[ProfileView]:
    for i in range(0, len(dids), 25):
        r = await client.bsky.actor.get_profiles({'actors': dids[i:i+25]})
        for p in r.profiles:
            yield simplify_profile_view(p)


async def no_connections(_client: AsyncClient, _did: str) -> AsyncIterable[ProfileView]:
    return
    yield


KNOWN_FURRIES_AND_CONNECTIONS = List[Tuple[Callable[[AsyncClient, str], AsyncIterable[ProfileView]], str]]


@dataclass
class StoreUser:
    user: ProfileView


@dataclass
class StorePost:
    post: FeedViewPost


async def store_to_db_task(db: Database, q: 'asyncio.Queue[Union[StoreUser, StorePost]]'):
    while True:
        item = await q.get()
        try:
            if isinstance(item, StoreUser):
                # print('Storing user', item.user.handle, '/', q.qsize())
                user = item.user
                gender = guess_gender_reductive(user.description) if user.description is not None else 'unknown'
                await db.actor.upsert(
                    where={'did': user.did},
                    data={
                        'create': {
                            'did': user.did,
                            'handle': user.handle,
                            'description': user.description,
                            'displayName': user.displayName,
                            'in_fox_feed': True,
                            'in_vix_feed': (gender == 'girl'),
                            'gender_label_auto': gender,
                        },
                        'update': {
                            'did': user.did,
                            'handle': user.handle,
                            'description': user.description,
                            'displayName': user.displayName,
                            'in_fox_feed': True,
                            'in_vix_feed': (gender == 'girl'),
                            'gender_label_auto': gender,
                        }
                    }
                )
            elif isinstance(item, StorePost):
                # print('Storing post', item.post.post.uri, '/', q.qsize())
                post = item.post
                p = post.post
                reply_parent = None if post.reply is None else post.reply.parent.uri
                reply_root = None if post.reply is None else post.reply.root.uri
                media_count = (
                    0 if not isinstance(p.embed, images.View)
                    else len(p.embed.images)
                )
                # if verbose:
                #     print(f'- ({p.uri}, {media_count} images, {p.likeCount or 0} likes) - {p.record["text"]}')
                await db.post.upsert(
                    where={'uri': p.uri},
                    data={
                        'create': {
                            'uri': p.uri,
                            'cid': p.cid,
                            # TODO: Fix these
                            'reply_parent': reply_parent,
                            'reply_root': reply_root,
                            'indexed_at': parse_datetime(p.record['createdAt']),
                            'like_count': p.likeCount or 0,
                            'authorId': p.author.did,
                            'mentions_fursuit': mentions_fursuit(p.record['text']),
                            'media_count': media_count,
                            'text': p.record['text'],
                        },
                        'update': {
                            'like_count': p.likeCount or 0,
                            'media_count': media_count,
                            'mentions_fursuit': mentions_fursuit(p.record['text']),
                            'text': p.record['text'],
                        }
                    }
                )
            else:
                pass
        finally:
            q.task_done()


async def load_posts_task(
        client: AsyncClient,
        only_posts_after: datetime,
        input_queue: 'asyncio.Queue[ProfileView]',
        output_queue: 'asyncio.Queue[Union[StorePost, StoreUser]]',
        *,
        actually_do_shit: bool = True
):
    cprint('Grabbing posts for furries...', 'blue', force_color=True)
    while True:
        user = await input_queue.get()
        try:
            if actually_do_shit:
                # cprint(f'Getting posts for {user.handle}', 'blue', force_color=True)
                async for post in get_posts(client, user.did, after=only_posts_after):
                    score = server.algos.score_task._raw_score(
                        datetime.now() - parse_datetime(post.post.indexedAt),
                        post.post.likeCount or 0,
                    )
                    if post.reply is None and score > SCORE_REQUIREMENT:
                        await output_queue.put(StorePost(post))
        except Exception:
            cprint(f'error while getting posts for user {user.handle}', color='red', force_color=True)
            traceback.print_exc()
        finally:
            input_queue.task_done()


async def log_queue_size_task(
        storage_queue: 'asyncio.Queue[Union[StorePost, StoreUser]]' ,
        post_load_queue: 'asyncio.Queue[ProfileView]',
) -> None:
    while True:
        print(f'> Load: {post_load_queue.qsize()} . Store: {storage_queue.qsize()}')
        await asyncio.sleep(30)


async def find_furries_raw(client: AsyncClient) -> AsyncIterable[ProfileView]:
    known_furries: KNOWN_FURRIES_AND_CONNECTIONS = [
        (get_people_who_like_your_feeds, 'puppyfox.bsky.social'),
        (get_follows, 'puppyfox.bsky.social'),
        (get_follows, 'furryli.st'),
        (get_mutuals, 'brae.gay'),
        (get_mutuals, '100racs.bsky.social'),
        (get_mutuals, 'glitzyfox.bsky.social'),
        (get_mutuals, 'itswolven.bsky.social'),
        (get_mutuals, 'coolkoinu.bsky.social'),
        (get_mutuals, 'gutterbunny.bsky.social'),
        (get_mutuals, 'zoeydogy.bsky.social'),
    ]

    cprint('Loading furries from seed list', 'blue', force_color=True)

    with gzip.open('./seed.json.gzip', 'rb') as sff:
        seed_list = json.loads(sff.read().decode('utf-8'))['seed']

    async for profile in get_many_profiles(client, seed_list):
        yield profile

    cprint('Grabbing furry-adjacent accounts', 'blue', force_color=True)

    for get_associations, handle in known_furries:
        profile = simplify_profile_view(await client.bsky.actor.get_profile({'actor': handle}))
        yield profile
        async for other in get_associations(client, profile.did):
            yield other


async def find_furries_clean(client: AsyncClient, mutes: Set[str]) -> AsyncIterable[ProfileView]:
    seen: Set[str] = set()
    async for profile in find_furries_raw(client):
        if profile.did not in seen and profile.did not in mutes:
            seen.add(profile.did)
            yield profile


async def load(db: Database, load_posts: bool = True) -> None:
    client = AsyncClient()
    await client.login(HANDLE, PASSWORD)

    only_posts_after = datetime.now() - server.algos.fox_feed.LOOKBACK_HARD_LIMIT

    cprint('Getting muted accounts', 'blue', force_color=True)
    mutes = {i.did async for _, i in get_all_mutes(client)} - {'' if client.me is None else client.me.did}

    storage_queue: 'asyncio.Queue[Union[StorePost, StoreUser]]' = asyncio.Queue()
    post_load_queue: 'asyncio.Queue[ProfileView]' = asyncio.Queue()

    storage_worker = asyncio.create_task(store_to_db_task(db, storage_queue))
    load_posts_worker = asyncio.create_task(load_posts_task(client, only_posts_after, post_load_queue, storage_queue, actually_do_shit=load_posts))
    report_task = asyncio.create_task(log_queue_size_task(storage_queue, post_load_queue))

    async for furry in find_furries_clean(client, mutes):
        # The posts for a user can be loaded before the user is stored, however the StoreUser will always be ahead of the relevant
        # StorePosts, so this will never break the DB foreign keys
        await storage_queue.put(StoreUser(furry))
        await post_load_queue.put(furry)

    cprint('Waiting for workers to finish...', 'blue', force_color=True)
    print(f'> Load: {post_load_queue.qsize()} . Store: {storage_queue.qsize()}')

    await post_load_queue.join()
    await storage_queue.join()
    storage_worker.cancel()
    load_posts_worker.cancel()
    report_task.cancel()
    await asyncio.gather(storage_worker, load_posts_worker, report_task, return_exceptions=True)

    cprint('Ok! Yeah! Woooo!', 'blue', force_color=True)
    cprint('Done scraping website :)', 'green', force_color=True)


async def main():
    db = await make_database_connection()
    await load(db, load_posts=True)


if __name__ == '__main__':
    asyncio.run(main())

