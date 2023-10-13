import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass

import atproto
import atproto.exceptions
from foxfeed.database import Database
from typing import (
    Callable,
    Coroutine,
    TypeVar,
    Any,
    List,
    Optional,
    Protocol,
    AsyncIterable,
    Union,
    Dict,
)
import time
from foxfeed.util import sleep_on, chunkify, achunkify, groupby, alist


from atproto.xrpc_client.models.app.bsky.actor.defs import (
    ProfileView,
    ProfileViewDetailed,
)
from atproto.xrpc_client.models.app.bsky.feed.defs import (
    FeedViewPost,
    GeneratorView,
)
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView, ListItemView
from atproto.xrpc_client.models.app.bsky.feed.get_likes import Like
from atproto.xrpc_client.models.app.bsky.feed.defs import PostView
from atproto import models


class Policy:

    def __init__(self, stop_event: asyncio.Event, ratelimit_timespan: timedelta, ratelimit_limit: int):
        self.stop_event = stop_event
        self.ratelimit_timespan = ratelimit_timespan
        self.ratelimit_limit = ratelimit_limit
        self.reset_at = datetime.now() - ratelimit_timespan
        self.counter = 0
        self.lock = asyncio.Lock()

    async def count_and_wait(self):
        async with self.lock:
            now = datetime.now()
            if now > self.reset_at:
                self.reset_at = now + self.ratelimit_timespan
                self.counter = 0
            self.counter += 1
            if self.counter > self.ratelimit_limit:
                await sleep_on(self.stop_event, (self.reset_at - now).total_seconds())


PolicyType = Optional[Union[asyncio.Event, Policy]]


async def count_and_wait_if_policy(policy: PolicyType) -> None:
    if isinstance(policy, Policy):
        await policy.count_and_wait()


async def sleep_on_stop_event(policy_or_stop_event: PolicyType, timeout: float) -> None:
    if isinstance(policy_or_stop_event, Policy):
        await sleep_on(policy_or_stop_event.stop_event, timeout)
    if isinstance(policy_or_stop_event, asyncio.Event):
        await sleep_on(policy_or_stop_event, timeout)


def ev_set(policy_or_stop_event: PolicyType) -> bool:
    if isinstance(policy_or_stop_event, Policy):
        return policy_or_stop_event.stop_event.is_set()
    if isinstance(policy_or_stop_event, asyncio.Event):
        return policy_or_stop_event.is_set()
    return False


B32CHARS = 'abcdefghijklmnopqrstuvwxyz234567'


def b32_decode(s: str) -> int:
    return sum((32 ** i) * B32CHARS.index(v) for i, v in enumerate(s[::-1]))


def b32_encode(x: int) -> str:
    s = ''
    while x > 0:
        s += B32CHARS[x % 32]
        x //= 32
    return s[::-1]


def get_adjacent_key(rkey: str) -> str:
    return b32_encode(b32_decode(rkey) - 1)


T = TypeVar("T")
U = TypeVar("U")


AsyncClient = atproto.AsyncClient


async def _login_from_handle_and_password(handle: str, password: str) -> AsyncClient:
    print("Attempting login with handle and password:", handle)
    client = AsyncClient()
    await client.login(login=handle, password=password)
    return client


async def _login_from_session_string(string: str) -> AsyncClient:
    print("Attempting login using session string")
    client = AsyncClient()
    await client.login(session_string=string)
    return client


async def _login_from_best_source(db: Database, handle: str, password: str) -> AsyncClient:
    session = await db.blueskyclientsession.find_first(
        where={"handle": handle},
        order={"created_at": "desc"},
    )
    try:
        assert session is not None
        client = await _login_from_session_string(session.session_string)
    except:
        client = await _login_from_handle_and_password(handle, password)
    await db.blueskyclientsession.delete_many(where={"handle": handle})
    await db.blueskyclientsession.create(
        data={"handle": handle, "session_string": client.export_session_string()}
    )
    return client


async def make_bsky_client(db: Database, handle: str, password: str) -> AsyncClient:
    return await _login_from_best_source(db, handle, password)


async def request_and_retry_on_ratelimit(
    function: Callable[[U], Coroutine[Any, Any, T]],
    argument: U,
    *,
    max_attempts: int,
    policy: PolicyType,
) -> Optional[T]:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for _ in range(max_attempts - 1):
        if ev_set(policy):
            return None
        try:
            await count_and_wait_if_policy(policy)
            return await function(argument)
        except atproto.exceptions.RequestException as e:
            if e.response and e.response.status_code in (500, 502):
                await sleep_on_stop_event(policy, 5)
            elif e.response and e.response.status_code == 429:
                if "ratelimit-reset" in e.response.headers:
                    time_to_wait = int(e.response.headers["ratelimit-reset"]) - time.time() + 1
                else:
                    time_to_wait = 10
                await sleep_on_stop_event(policy, time_to_wait)
            else:
                raise
    if ev_set(policy):
        return None
    return await function(argument)


class ModelWithCursor(Protocol):
    cursor: Optional[str]
    def model_copy(self, *, update: Dict[str, Any]) -> 'ModelWithCursor': ...


QueryParams = TypeVar("QueryParams", bound=ModelWithCursor)
QueryResult = TypeVar("QueryResult", bound=ModelWithCursor)


async def paginate(
    params: QueryParams,
    query: Callable[[QueryParams], Coroutine[Any, Any, QueryResult]],
    getval: Callable[[QueryResult], List[U]],
    *,
    policy: PolicyType,
) -> AsyncIterable[U]:
    if ev_set(policy):
        return
    r = await request_and_retry_on_ratelimit(
        query, params, max_attempts=3, policy=policy
    )
    if r is not None:
        for i in getval(r):
            yield i
    while r is not None and r.cursor is not None and not ev_set(policy):
        r = await request_and_retry_on_ratelimit(
            query,
            params.model_copy(update={"cursor": r.cursor}),
            max_attempts=3,
            policy=policy,
        )
        if r is not None:
            for i in getval(r):
                yield i


def get_followers(
    client: AsyncClient, did: str, policy: PolicyType = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetFollowers.Params(actor=did),
        client.app.bsky.graph.get_followers,
        lambda r: r.followers,
        policy=policy,
    )


def get_follows(
    client: AsyncClient, did: str, policy: PolicyType = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetFollows.Params(actor=did),
        client.app.bsky.graph.get_follows,
        lambda r: r.follows,
        policy=policy,
    )


def get_feeds(
    client: AsyncClient, did: str, policy: PolicyType = None
) -> AsyncIterable[GeneratorView]:
    return paginate(
        models.AppBskyFeedGetActorFeeds.Params(actor=did),
        client.app.bsky.feed.get_actor_feeds,
        lambda r: r.feeds,
        policy=policy,
    )


def get_posts(
    client: AsyncClient, did: str, policy: PolicyType = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetAuthorFeed.Params(actor=did),
        client.app.bsky.feed.get_author_feed,
        lambda r: r.feed,
        policy=policy,
    )


async def get_specific_posts(
    client: AsyncClient, uris: List[str], policy: PolicyType = None
) -> AsyncIterable[PostView]:
    for block in chunkify(uris, 25):
        if ev_set(policy):
            return
        posts = await request_and_retry_on_ratelimit(
            client.app.bsky.feed.get_posts,
            models.AppBskyFeedGetPosts.Params(uris=block),
            max_attempts=3,
            policy=policy
        )
        if posts is not None:
            for i in posts.posts:
                yield i


async def get_specific_profiles(
    client: AsyncClient, dids: List[str], policy: PolicyType = None
) -> AsyncIterable[ProfileViewDetailed]:
    for block in chunkify(dids, 25):
        if ev_set(policy):
            return
        users = await request_and_retry_on_ratelimit(
            client.app.bsky.actor.get_profiles,
            models.AppBskyActorGetProfiles.Params(actors=block),
            max_attempts=3,
            policy=policy
        )
        if users is not None:
            for i in users.profiles:
                yield i


@dataclass
class LikeWithDeets:
    uri: str
    cid: Optional[str]
    post_uri: str
    post_cid: str
    actor_did: str
    created_at: str


async def get_single_like(
    client: AsyncClient, repo: str, collection: str, record: str, policy: PolicyType = None
) -> Optional[LikeWithDeets]:
    try:
        response = await request_and_retry_on_ratelimit(
            client.com.atproto.repo.get_record,
            models.ComAtprotoRepoGetRecord.Params(
                repo=repo,
                collection=collection,
                rkey=record,
            ),
            max_attempts=3,
            policy=policy
        )
    except atproto.exceptions.BadRequestError as e:
        if e.response is not None and e.response.status_code == 400:
            pass
        else:
            raise
    else:
        if response is not None:
            assert isinstance(response.value, models.AppBskyFeedLike.Main)
            return LikeWithDeets(
                uri=response.uri,
                cid=response.cid,
                post_uri=response.value.subject.uri,
                post_cid=response.value.subject.cid,
                actor_did=repo,
                created_at=response.value.created_at
            )


async def _list_records_from_repo(
    client: AsyncClient, repo: str, collection: str, keys: List[str], policy: PolicyType = None
) -> AsyncIterable[models.ComAtprotoRepoListRecords.Record]:
    keys_to_find = set(keys)
    for rkey in sorted(keys):
        if rkey not in keys_to_find:
            continue
        try:
            response = await request_and_retry_on_ratelimit(
                client.com.atproto.repo.list_records,
                models.ComAtprotoRepoListRecords.Params(
                    repo=repo,
                    collection=collection,
                    rkeyStart=get_adjacent_key(rkey),
                    limit=100,
                    # By default the query goes high-to-low keys (travels backways through time), but we want to go forward
                    reverse=True,
                ),
                max_attempts=3,
                policy=policy
            )
        except atproto.exceptions.BadRequestError as e:
            if e.response is not None and e.response.status_code == 400:
                # repo not found, something's been deleted, just exit
                return
            raise e
        else:
            if response is None:
                break
            for rec in response.records:
                response_rkey = rec.uri.split('/')[-1]
                if response_rkey in keys_to_find:
                    keys_to_find.remove(response_rkey)
                    yield rec


async def _list_records(
    client: AsyncClient, uris: List[str], policy: PolicyType = None
) -> AsyncIterable[models.ComAtprotoRepoListRecords.Record]:
    grouped = groupby(
        lambda x: (x[0], x[1]),
        [i.split('/')[2:5] for i in uris]
    )
    sources: List[Coroutine[Any, Any, List[models.ComAtprotoRepoListRecords.Record]]] = [
        alist(_list_records_from_repo(client, repo, collection, [i[2] for i in x], policy))
        for (repo, collection), x in grouped.items()
    ]
    for ls in await asyncio.gather(*sources):
        for i in ls:
            yield i


async def get_specific_likes(
    client: AsyncClient, uris: List[str], policy: PolicyType = None
) -> AsyncIterable[LikeWithDeets]:
    async for i in _list_records(client, uris, policy):
        assert isinstance(i.value, models.AppBskyFeedLike.Main)
        yield LikeWithDeets(
            uri=i.uri,
            cid=i.cid,
            post_uri=i.value.subject.uri,
            post_cid=i.value.subject.cid,
            actor_did=i.uri.split('/')[2],
            created_at=i.value.created_at,
        )


def get_likes(
    client: AsyncClient, uri: str, policy: PolicyType = None
) -> AsyncIterable[Like]:
    return paginate(
        models.AppBskyFeedGetLikes.Params(uri=uri),
        client.app.bsky.feed.get_likes,
        lambda r: r.likes,
        policy=policy,
    )


def get_actor_likes(
    client: AsyncClient, actor: str, policy: PolicyType = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetActorLikes.Params(actor=actor),
        client.app.bsky.feed.get_actor_likes,
        lambda r: r.feed,
        policy=policy,
    )


def get_mute_lists(
    client: AsyncClient, policy: PolicyType = None
) -> AsyncIterable[ListView]:
    return paginate(
        models.AppBskyGraphGetListMutes.Params(),
        client.app.bsky.graph.get_list_mutes,
        lambda r: r.lists,
        policy=policy,
    )


def get_mutes(
    client: AsyncClient, policy: PolicyType = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetMutes.Params(),
        client.app.bsky.graph.get_mutes,
        lambda r: r.mutes,
        policy=policy,
    )


def get_list(
    client: AsyncClient, uri: str, policy: PolicyType = None
) -> AsyncIterable[ListItemView]:
    return paginate(
        models.AppBskyGraphGetList.Params(list=uri),
        client.app.bsky.graph.get_list,
        lambda r: r.items,
        policy=policy,
    )


async def as_detailed_profiles(
    client: AsyncClient,
    func: Callable[
        [AsyncClient, str, PolicyType],
        AsyncIterable[ProfileView],
    ],
    arg: str,
    policy: PolicyType = None
) -> AsyncIterable[ProfileViewDetailed]:
    async for chunk in achunkify(func(client, arg, policy), 25):
        if ev_set(policy):
            return
        async for i in get_specific_profiles(client, [i.did for i in chunk], policy):
            yield i
