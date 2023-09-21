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
from foxfeed.util import sleep_on


from atproto.xrpc_client.models.app.bsky.actor.defs import (
    ProfileView,
)
from atproto.xrpc_client.models.app.bsky.feed.defs import (
    FeedViewPost,
    GeneratorView,
)
from atproto.xrpc_client.models.app.bsky.graph.defs import ListView, ListItemView
from atproto.xrpc_client.models.app.bsky.feed.get_likes import Like
from atproto.xrpc_client.models.app.bsky.feed.defs import PostView
from atproto import models


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
        return await _login_from_session_string(session.session_string)
    except:
        client = await _login_from_handle_and_password()
        await db.blueskyclientsession.create(
            data={"handle": HANDLE, "session_string": client.export_session_string()}
        )
        return client


async def make_bsky_client(db: Database) -> AsyncClient:
    return await _login_from_best_source(db)


async def request_and_retry_on_ratelimit(
    max_attempts: int,
    f: Callable[[U], Coroutine[Any, Any, T]],
    a: U,
    *,
    stop_event: Optional[asyncio.Event] = None,
) -> T:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for _ in range(max_attempts - 1):
        if stop_event is not None and stop_event.is_set():
            break
        try:
            return await f(a)
        except atproto.exceptions.RequestException as e:
            if e.response and e.response.status_code == 500:
                await (
                    asyncio.sleep(2) if stop_event is None else sleep_on(stop_event, 2)
                )
            elif e.response and e.response.status_code == 429:
                reset_at = int(e.response.headers.get("ratelimit-reset", None))
                time_to_wait = reset_at - time.time() + 1
                await (
                    asyncio.sleep(time_to_wait)
                    if stop_event is None
                    else sleep_on(stop_event, time_to_wait)
                )
            else:
                raise
    return await f(a)


class HasCursor(Protocol):
    cursor: Optional[str]


from pydantic import BaseModel


QueryParams = TypeVar("QueryParams", bound=BaseModel)
QueryResult = TypeVar("QueryResult", bound=HasCursor)


async def paginate(
    start_query: QueryParams,
    query: Callable[[QueryParams], Coroutine[Any, Any, QueryResult]],
    getval: Callable[[QueryResult], List[U]],
    *,
    stop_event: Optional[asyncio.Event] = None,
) -> AsyncIterable[U]:
    if stop_event is not None and stop_event.is_set():
        return
    r = await request_and_retry_on_ratelimit(
        3, query, start_query, stop_event=stop_event
    )
    for i in getval(r):
        yield i
    while r.cursor is not None and (stop_event is None or not stop_event.is_set()):
        r = await request_and_retry_on_ratelimit(
            3,
            query,
            start_query.model_copy(update={"cursor": r.cursor}),
            stop_event=stop_event,
        )
        for i in getval(r):
            yield i


def get_followers(
    client: AsyncClient, did: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        # {"actor": did},
        models.AppBskyGraphGetFollowers.Params(actor=did),
        client.app.bsky.graph.get_followers,
        lambda r: r.followers,
        stop_event=stop_event,
    )


def get_follows(
    client: AsyncClient, did: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetFollows.Params(actor=did),
        client.app.bsky.graph.get_follows,
        lambda r: r.follows,
        stop_event=stop_event,
    )


def get_feeds(
    client: AsyncClient, did: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[GeneratorView]:
    return paginate(
        models.AppBskyFeedGetActorFeeds.Params(actor=did),
        client.app.bsky.feed.get_actor_feeds,
        lambda r: r.feeds,
        stop_event=stop_event,
    )


def get_posts(
    client: AsyncClient, did: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetAuthorFeed.Params(actor=did),
        client.app.bsky.feed.get_author_feed,
        lambda r: r.feed,
        stop_event=stop_event,
    )


async def get_specific_posts(
    client: AsyncClient, uris: List[str], *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[PostView]:
    for i in range(0, len(uris), 25):
        if stop_event is not None and stop_event.is_set():
            return
        block = uris[i:i+25]
        posts = await request_and_retry_on_ratelimit(
            3,
            client.app.bsky.feed.get_posts,
            models.AppBskyFeedGetPosts.Params(uris=block),
            stop_event=stop_event
        )
        for i in posts.posts:
            yield i


def get_likes(
    client: AsyncClient, uri: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[Like]:
    return paginate(
        models.AppBskyFeedGetLikes.Params(uri=uri),
        client.app.bsky.feed.get_likes,
        lambda r: r.likes,
        stop_event=stop_event,
    )


def get_actor_likes(
    client: AsyncClient, actor: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[FeedViewPost]:
    return paginate(
        models.AppBskyFeedGetActorLikes.Params(actor=actor),
        client.app.bsky.feed.get_actor_likes,
        lambda r: r.feed,
        stop_event=stop_event,
    )


def get_mute_lists(
    client: AsyncClient, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ListView]:
    return paginate(
        models.AppBskyGraphGetListMutes.Params(),
        client.app.bsky.graph.get_list_mutes,
        lambda r: r.lists,
        stop_event=stop_event,
    )


def get_mutes(
    client: AsyncClient, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ProfileView]:
    return paginate(
        models.AppBskyGraphGetMutes.Params(),
        client.app.bsky.graph.get_mutes,
        lambda r: r.mutes,
        stop_event=stop_event,
    )


def get_list(
    client: AsyncClient, uri: str, *, stop_event: Optional[asyncio.Event] = None
) -> AsyncIterable[ListItemView]:
    return paginate(
        models.AppBskyGraphGetList.Params(list=uri),
        client.app.bsky.graph.get_list,
        lambda r: r.items,
        stop_event=stop_event,
    )
