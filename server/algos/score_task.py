import asyncio
import traceback
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import server.database
from server.database import Database, make_database_connection, ScorePostsOutputModel
from prisma.models import Post, Actor
import prisma.errors
import prisma.types
import server.gen.db

from typing import List, Dict, Tuple, Callable, Iterator, Literal, Set, Optional
from .feed_names import FeedName

import server.gender

from termcolor import cprint
from dataclasses import dataclass
import gc
from server.util import sleep_on
import sys


from server.store import store_post2
from atproto import AsyncClient
from server.bsky import make_bsky_client


LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=12)
FRESH_CURVE_INFLECTION_POINT = timedelta(minutes=30)
ALPHA = 1.5


PostScoreResult = ScorePostsOutputModel


def do_not_remix(ps: List[PostScoreResult]) -> List[PostScoreResult]:
    return ps


@dataclass
class StandardDecay:
    alpha: float
    beta: str
    gamma: float


@dataclass
class FeedParameters:
    feed_name: FeedName
    decay: Optional[StandardDecay]
    include_guy_posts: bool
    include_guy_votes: bool
    # lmt: int
    remix_func: Callable[
        [List[PostScoreResult]], List[PostScoreResult]
    ] = do_not_remix


@dataclass
class RunDetails:
    run_starttime: datetime
    run_version: int


def sorted_by_score_desc(
    posts: List[PostScoreResult]
) -> List[PostScoreResult]:
    return sorted(posts, key=lambda x: x.score, reverse=True)


# def has_trans_vibes(i: Actor) -> bool:
#     s = ((i.displayName or "") + " " + (i.description or "")).lower()
#     return "🏳️‍⚧️" in s or "trans" in s


# def has_masc_vibes(i: Actor) -> bool:
#     return (
#         i.manual_include_in_vix_feed is not True
#         and i.autolabel_masc_vibes
#         and not i.autolabel_fem_vibes
#     )


# def has_fem_vibes(i: Actor) -> bool:
#     return i.manual_include_in_vix_feed is True or (
#         i.manual_include_in_vix_feed is None
#         and i.autolabel_fem_vibes
#         and not i.autolabel_masc_vibes
#     )


# def _gender_splitmix(p: List[Tuple[float, PostWithInfo]]) -> Iterator[PostWithInfo]:
#     # Not a huge fan of this approach but uhhhhhhhh
#     transfem = [
#         i for _, i in p if has_trans_vibes(i.author) and has_fem_vibes(i.author)
#     ]
#     cisfem = [
#         i for _, i in p if not has_trans_vibes(i.author) and has_fem_vibes(i.author)
#     ]
#     transmasc = [
#         i for _, i in p if has_trans_vibes(i.author) and has_masc_vibes(i.author)
#     ]
#     cismasc = [
#         i for _, i in p if not has_trans_vibes(i.author) and has_masc_vibes(i.author)
#     ]
#     other = [
#         i for _, i in p if not has_fem_vibes(i.author) and not has_masc_vibes(i.author)
#     ]
#     lists = [transfem, cisfem, transmasc, cismasc, other]
#     for i in range(max(map(len, lists))):
#         for x in lists:
#             if i < len(x):
#                 yield x[i]


# def gender_splitmix(ps: List[Tuple[float, PostWithInfo]]) -> List[PostWithInfo]:
#     return list(_gender_splitmix(ps))


# def actor_is_fem(actor: Actor):
#     return actor.manual_include_in_vix_feed is True or (
#         actor.manual_include_in_vix_feed is None
#         and actor.autolabel_fem_vibes is True
#         and actor.autolabel_masc_vibes is False
#     )


def post_is_masc_nsfw(p: PostScoreResult):
    return bool(p.labels) and not p.author_is_fem


def _masc_nsfw_limiter(ratio: int, ps: List[PostScoreResult]) -> Iterator[PostScoreResult]:
    masc_nsfw = [i for i in ps if post_is_masc_nsfw(i)][::-1]
    other = [i for i in ps if not post_is_masc_nsfw(i)][::-1]
    while other and masc_nsfw:
        for _ in range(ratio):
            if other:
                yield other.pop()
        if other and masc_nsfw and masc_nsfw[-1].score > other[-1].score:
            yield masc_nsfw.pop()
    while other:
        yield other.pop()
    while masc_nsfw:
        yield masc_nsfw.pop()


def masc_nsfw_limiter(ratio: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) -> Callable[[List[PostScoreResult]], List[PostScoreResult]]:
    def _f(ps: List[PostScoreResult]) -> List[PostScoreResult]:
        return list(_masc_nsfw_limiter(ratio, ps))
    return _f


def top_100_chronological(p: List[PostScoreResult]) -> List[PostScoreResult]:
    return sorted(
        p[:100], key=lambda p: p.indexed_at, reverse=True
    )


score_time_decay = StandardDecay(
    alpha=1.5,
    beta='12 hours',
    gamma=0.9,
)


fast_time_decay = StandardDecay(
    alpha=1.5,
    beta='30 minutes',
    gamma=0.3,
)


ALGORITHMIC_FEEDS = [
    FeedParameters(
        feed_name="fox-feed",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=True,
        remix_func=masc_nsfw_limiter(4),
    ),
    FeedParameters(
        feed_name="vix-feed",
        decay=score_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
    ),
    FeedParameters(
        feed_name="fresh-feed",
        decay=fast_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
    ),
    FeedParameters(
        feed_name="vix-votes",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=False,
        remix_func=masc_nsfw_limiter(4),
    ),
    # FeedParameters(
    #     feed_name="bisexy",
    #     filter=lambda p: post_is_irl_nsfw(p) and p.post.media_count > 0,
    #     score_func=raw_score,
    #     remix_func=gender_splitmix,
    # ),
    FeedParameters(
        feed_name="top-feed",
        decay=None,
        include_guy_posts=False,
        include_guy_votes=True,
        remix_func=top_100_chronological,
    ),
]


async def create_feed(db: Database, fp: FeedParameters, rd: RunDetails) -> Set[str]:
    decay = fp.decay or StandardDecay(alpha=1, beta='1 hour', gamma=1)
    scored_posts = await server.gen.db.score_posts(
        db,
        alpha=decay.alpha,
        beta=decay.beta,
        gamma=decay.gamma,
        do_time_decay=fp.decay is not None,
        include_guy_posts=fp.include_guy_posts,
        include_guy_votes=fp.include_guy_votes,
        lmt=1000,
    )

    will_store = fp.remix_func(scored_posts)[:500]

    cprint(
        f"Scoring {fp.feed_name}::{rd.run_version} has resulted in {len(will_store)} scored posts",
        "yellow",
        force_color=True,
    )

    blob: List[prisma.types.PostScoreCreateWithoutRelationsInput] = [
        {
            "uri": post.uri,
            "version": rd.run_version,
            # This is actually "rank"
            "score": len(will_store) - i,
            "created_at": rd.run_starttime,
            "feed_name": fp.feed_name,
        }
        for i, post in enumerate(will_store)
    ]

    try:
        await db.postscore.create_many(data=blob)
    except prisma.errors.UniqueViolationError:
        cprint(f"Unique violation when creating PostScores on {fp.feed_name}::{rd.run_version}", 'red', force_color=True)

    return {i.uri for i in will_store}


async def score_posts(shutdown_event: asyncio.Event, db: Database, client: AsyncClient, do_refresh_posts: bool = False) -> None:
    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f"Starting scoring round {run_version}", "yellow", force_color=True)

    rd = RunDetails(
        run_starttime=run_starttime, run_version=run_version
    )

    all_uris_in_feeds: Set[str] = set()
    for algo in ALGORITHMIC_FEEDS:
        all_uris_in_feeds |= await create_feed(db, algo, rd)
        if shutdown_event.is_set():
            break

    run_endtime = datetime.now(tz=timezone.utc)

    cprint(
        f"Scoring round {run_version} took {(run_endtime - run_starttime).seconds // 60} minutes",
        "yellow",
        force_color=True,
    )

    if shutdown_event.is_set():
        cprint("It ended early due to a shutdown event", "yellow", force_color=True)
        return

    await db.postscore.delete_many(
        where={"created_at": {"lt": run_starttime - timedelta(hours=1)}}
    )

    if do_refresh_posts:
        posts_to_refresh = await db.post.find_many(
            take=500,
            where={
                'uri': {'in': list(all_uris_in_feeds)},
                'indexed_at': {'lt': run_endtime - timedelta(minutes=20)},
                'OR': [
                    {'last_rescan': None},
                    {'last_rescan': {'lt': run_endtime - timedelta(hours=6)}},
                ]
            }
        )
        if posts_to_refresh:
            print(f'Refreshing {len(posts_to_refresh)} posts')
            for i in range(0, len(posts_to_refresh), 25):
                block = posts_to_refresh[i:i+25]
                refreshed = await client.app.bsky.feed.get_posts({'uris': [i.uri for i in block]})
                for i in refreshed.posts:
                    await store_post2(db, i, None, None, now=run_endtime)
            print('Refresh done')


async def score_posts_forever(shutdown_event: asyncio.Event, db: Database, client: AsyncClient):
    while not shutdown_event.is_set():
        try:
            await score_posts(shutdown_event, db, client, do_refresh_posts=True)
            cprint(f"gc-d {gc.collect()} objects", "yellow", force_color=True)
        except Exception:
            cprint(f"Error during score_posts", color="red", force_color=True)
            traceback.print_exc()
        await sleep_on(shutdown_event, 60)


async def main(forever: bool):
    db = await make_database_connection(timeout=30)
    client = await make_bsky_client(db)
    event = asyncio.Event()
    if forever:
        await score_posts_forever(event, db, client)
    else:
        await score_posts(event, db, client, do_refresh_posts=True)


if __name__ == "__main__":
    forever = "--forever" in sys.argv
    asyncio.run(main(forever))
