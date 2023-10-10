import asyncio
from datetime import datetime, timedelta

import atproto
import atproto.exceptions
from foxfeed.config import HANDLE, PASSWORD
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
from foxfeed.util import sleep_on, chunkify, achunkify


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


T = TypeVar("T")
U = TypeVar("U")


AsyncClient = atproto.AsyncClient


async def _login_from_handle_and_password() -> AsyncClient:
    print("Attempting login with handle and password")
    client = AsyncClient()
    await client.login(login=HANDLE, password=PASSWORD)
    return client


async def _login_from_session_string(string: str) -> AsyncClient:
    print("Attempting login using session string")
    client = AsyncClient()
    await client.login(session_string=string)
    return client


async def _login_from_best_source(db: Database) -> AsyncClient:
    session = await db.blueskyclientsession.find_first(
        where={"handle": HANDLE},
        order={"created_at": "desc"},
    )
    try:
        assert session is not None
        client = await _login_from_session_string(session.session_string)
    except:
        client = await _login_from_handle_and_password()
    await db.blueskyclientsession.delete_many(where={"handle": HANDLE})
    await db.blueskyclientsession.create(
        data={"handle": HANDLE, "session_string": client.export_session_string()}
    )
    return client


async def make_bsky_client(db: Database) -> AsyncClient:
    return await _login_from_best_source(db)


async def request_and_retry_on_ratelimit(
    function: Callable[[U], Coroutine[Any, Any, T]],
    argument: U,
    *,
    max_attempts: int,
    policy: PolicyType,
) -> T:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for _ in range(max_attempts - 1):
        if ev_set(policy):
            break
        try:
            await count_and_wait_if_policy(policy)
            return await function(argument)
        except atproto.exceptions.RequestException as e:
            if e.response and e.response.status_code == 500:
                await sleep_on_stop_event(policy, 2)
            elif e.response and e.response.status_code == 429:
                reset_at = int(e.response.headers.get("ratelimit-reset", None))
                time_to_wait = reset_at - time.time() + 1
                await sleep_on_stop_event(policy, time_to_wait)
            else:
                raise
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
    for i in getval(r):
        yield i
    while r.cursor is not None and not ev_set(policy):
        r = await request_and_retry_on_ratelimit(
            query,
            params.model_copy(update={"cursor": r.cursor}),
            max_attempts=3,
            policy=policy,
        )
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
        for i in users.profiles:
            yield i


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
