import asyncio
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

from pydantic import BaseModel


# We use an event that will never be set as a "default" argument,
# makes the code cleaner than passing Optional[asyncio.Event]s around
def ev_set(stop_event: Optional[asyncio.Event] = None):
    return stop_event is not None and stop_event.is_set()


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
    stop_event: Optional[asyncio.Event],
) -> T:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for _ in range(max_attempts - 1):
        if ev_set(stop_event):
            break
        try:
            return await function(argument)
        except atproto.exceptions.RequestException as e:
            if e.response and e.response.status_code == 500:
                await sleep_on(stop_event, 2)
            elif e.response and e.response.status_code == 429:
                reset_at = int(e.response.headers.get("ratelimit-reset", None))
                time_to_wait = reset_at - time.time() + 1
                await sleep_on(stop_event, time_to_wait)
            else:
                raise
    return await function(argument)


class HasCursor(Protocol):
    cursor: Optional[str]


QueryParams = TypeVar("QueryParams", bound=BaseModel)
QueryResult = TypeVar("QueryResult", bound=HasCursor)


async def paginate(
    start_query: QueryParams,
    query: Callable[[QueryParams], Coroutine[Any, Any, QueryResult]],
    getval: Callable[[QueryResult], List[U]],
    *,
    stop_event: Optional[asyncio.Event],
) -> AsyncIterable[U]:
    if ev_set(stop_event):
        return
    r = await request_and_retry_on_ratelimit(
        query, start_query, max_attempts=3, stop_event=stop_event
    )
    for i in getval(r):
        yield i
    while r.cursor is not None and not ev_set(stop_event):
        r = await request_and_retry_on_ratelimit(
            query,
            start_query.model_copy(update={"cursor": r.cursor}),
            max_attempts=3,
            stop_event=stop_event,
        )
        for i in getval(r):
            yield i


def get_followers(
    client: AsyncClient, did: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetFollowers.Params(actor=did),
        client.app.bsky.graph.get_followers,
        lambda r: r.followers,
        stop_event=stop_event,
    )


def get_follows(
    client: AsyncClient, did: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetFollows.Params(actor=did),
        client.app.bsky.graph.get_follows,
        lambda r: r.follows,
        stop_event=stop_event,
    )


def get_feeds(
    client: AsyncClient, did: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[GeneratorView]:
    return paginate(
        models.AppBskyFeedGetActorFeeds.Params(actor=did),
        client.app.bsky.feed.get_actor_feeds,
        lambda r: r.feeds,
        stop_event=stop_event,
    )


def get_posts(
    client: AsyncClient, did: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetAuthorFeed.Params(actor=did),
        client.app.bsky.feed.get_author_feed,
        lambda r: r.feed,
        stop_event=stop_event,
    )


async def get_specific_posts(
    client: AsyncClient, uris: List[str], stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[PostView]:
    for block in chunkify(uris, 25):
        if ev_set(stop_event):
            return
        posts = await request_and_retry_on_ratelimit(
            client.app.bsky.feed.get_posts,
            models.AppBskyFeedGetPosts.Params(uris=block),
            max_attempts=3,
            stop_event=stop_event
        )
        for i in posts.posts:
            yield i


async def get_specific_profiles(
    client: AsyncClient, dids: List[str], stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileViewDetailed]:
    for block in chunkify(dids, 25):
        if ev_set(stop_event):
            return
        users = await request_and_retry_on_ratelimit(
            client.app.bsky.actor.get_profiles,
            models.AppBskyActorGetProfiles.Params(actors=block),
            max_attempts=3,
            stop_event=stop_event
        )
        for i in users.profiles:
            yield i


def get_likes(
    client: AsyncClient, uri: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[Like]:
    return paginate(
        models.AppBskyFeedGetLikes.Params(uri=uri),
        client.app.bsky.feed.get_likes,
        lambda r: r.likes,
        stop_event=stop_event,
    )


def get_actor_likes(
    client: AsyncClient, actor: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetActorLikes.Params(actor=actor),
        client.app.bsky.feed.get_actor_likes,
        lambda r: r.feed,
        stop_event=stop_event,
    )


def get_mute_lists(
    client: AsyncClient, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ListView]:
    return paginate(
        models.AppBskyGraphGetListMutes.Params(),
        client.app.bsky.graph.get_list_mutes,
        lambda r: r.lists,
        stop_event=stop_event,
    )


def get_mutes(
    client: AsyncClient, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetMutes.Params(),
        client.app.bsky.graph.get_mutes,
        lambda r: r.mutes,
        stop_event=stop_event,
    )


def get_list(
    client: AsyncClient, uri: str, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ListItemView]:
    return paginate(
        models.AppBskyGraphGetList.Params(list=uri),
        client.app.bsky.graph.get_list,
        lambda r: r.items,
        stop_event=stop_event,
    )


async def as_detailed_profiles(
    client: AsyncClient,
    func: Callable[
        [AsyncClient, str, Optional[asyncio.Event]],
        AsyncIterable[ProfileView],
    ],
    arg: str,
    stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileViewDetailed]:
    async for chunk in achunkify(func(client, arg, stop_event), 25):
        if ev_set(stop_event):
            return
        async for i in get_specific_profiles(client, [i.did for i in chunk], stop_event):
            yield i
