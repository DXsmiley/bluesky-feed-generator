import asyncio
import traceback
from datetime import datetime
from datetime import timezone
from datetime import timedelta

import server.database
from server.database import Database, make_database_connection
from prisma.models import Post, Actor
import prisma.errors
import prisma.types

from typing import List, Dict, Tuple, Callable, Iterator
from .feed_names import FeedName

import server.gender

from termcolor import cprint
from dataclasses import dataclass
import gc
from server.util import groupby
import sys


LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=12)
FRESH_CURVE_INFLECTION_POINT = timedelta(minutes=30)


def decay_curve(x: float) -> float:
    ALPHA = 1.5
    return (
        1 / (x ** ALPHA) if x > 1
        else (2 - (1 / ((2 - x) ** ALPHA)))
    )


async def get_like_counts(db: Database, run_starttime: datetime, filter: prisma.types.LikeWhereInput) -> Dict[str, int]:
    likes_aggregated = await db.like.group_by(
        by=['post_uri'],
        where={
            'created_at': {'lt': run_starttime, 'gt': run_starttime - LOOKBACK_HARD_LIMIT},
            'post': {'is': {'indexed_at': {'lt': run_starttime, 'gt': run_starttime - LOOKBACK_HARD_LIMIT}}},
            'AND': [filter],
        },
        count=True,
    )
    return {i.get('post_uri', ''): i.get('_count', {}).get('_all', 0) for i in likes_aggregated}


@dataclass
class PostWithInfo:
    post: Post
    author: Actor
    like_count_furries: int
    like_count_girls: int


async def load_all_posts(db: Database, run_starttime: datetime) -> List[PostWithInfo]:
    likes_by_furries = await get_like_counts(db, run_starttime, {'liker': {'is': server.database.user_is_in_fox_feed}})
    likes_by_girls = await get_like_counts(db, run_starttime, {'liker': {'is': server.database.user_is_in_vix_feed}})
    posts = await db.post.find_many(
        order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
        where={
            'reply_root': None,
            'indexed_at': {'lt': run_starttime, 'gt': run_starttime - LOOKBACK_HARD_LIMIT},
            'author': {'is': server.database.user_is_in_fox_feed},
        },
        include={
            'author': True,
        }
    )
    return [
        PostWithInfo(
            post=i,
            author=i.author,
            like_count_furries=likes_by_furries.get(i.uri, 0),
            like_count_girls=likes_by_girls.get(i.uri, 0)
        )
        for i in posts
        if i.author is not None
    ]


def penalty(p: PostWithInfo):
    vibes = server.gender.vibecheck(p.post.text)
    return (
        # Penalise people posting images without alt-text
        (0.7 if p.post.media_count > 0 and p.post.media_with_alt_text_count == 0 else 1.0)
        # TODO: boosting girl-vibes on the feed, assess the impact of this later
        * (1.1 if vibes.fem and not vibes.masc else 1.0)
        * (0.9 if vibes.masc and not vibes.fem else 1.0)
    )


def _raw_score(post_age: timedelta, like_count: int) -> float:
    # Number of likes, decaying over time
    # initial decay is much slower than the hacker news algo, but also decays to zero
    x = post_age / SCORING_CURVE_INFLECTION_POINT
    return (like_count ** 0.9 + 2) * decay_curve(max(0, x))


def raw_score(run_starttime: datetime, p: PostWithInfo) -> float:
    return _raw_score(run_starttime - p.post.indexed_at, p.like_count_furries) * penalty(p)


def raw_vix_vote_score(run_starttime: datetime, p: PostWithInfo) -> float:
    return (
        0 if p.like_count_girls < 3
        else _raw_score(run_starttime - p.post.indexed_at, p.like_count_girls) * penalty(p)
    )


def _raw_freshness(post_age: timedelta, like_count: int) -> float:
    # Number of likes, decaying over time
    # initial decay is much slower than the hacker news algo, but also decays to zero
    x = post_age / FRESH_CURVE_INFLECTION_POINT
    return (like_count ** 0.3 + 2) * decay_curve(max(0, x))


def raw_freshness(run_starttime: datetime, p: PostWithInfo) -> float:
    return _raw_freshness(run_starttime - p.post.indexed_at, p.like_count_furries) * penalty(p)


@dataclass
class FeedParameters:
    feed_name: FeedName
    filter: Callable[[PostWithInfo], bool]
    score_func: Callable[[datetime, PostWithInfo], float]
    remix_func: Callable[[List[Tuple[float, PostWithInfo]]], List[PostWithInfo]] = lambda p: [i for _, i in p]


@dataclass
class RunDetails:
    candidate_posts: List[PostWithInfo]
    run_starttime: datetime
    run_version: int


def sorted_by_score_desc(posts: List[Tuple[float, PostWithInfo]]) -> List[Tuple[float, PostWithInfo]]:
    return sorted(posts, key=lambda x: x[0], reverse=True)


def has_trans_vibes(i: Actor) -> bool:
    s = ((i.displayName or '') + ' ' + (i.description or '')).lower()
    return '🏳️‍⚧️' in s or 'trans' in s


def has_masc_vibes(i: Actor) -> bool:
    return i.manual_include_in_vix_feed is not True and i.autolabel_masc_vibes and not i.autolabel_fem_vibes


def has_fem_vibes(i: Actor) -> bool:
    return (
        i.manual_include_in_vix_feed is True
        or (
            i.manual_include_in_vix_feed is None
            and i.autolabel_fem_vibes
            and not i.autolabel_masc_vibes
        )
    )


def _gender_splitmix(p: List[Tuple[float, PostWithInfo]]) -> Iterator[PostWithInfo]:
    # Not a huge fan of this approach but uhhhhhhhh
    transfem = [i for _, i in p if has_trans_vibes(i.author) and has_fem_vibes(i.author)]
    cisfem = [i for _, i in p if not has_trans_vibes(i.author) and has_fem_vibes(i.author)]
    transmasc = [i for _, i in p if has_trans_vibes(i.author) and has_masc_vibes(i.author)]
    cismasc = [i for _, i in p if not has_trans_vibes(i.author) and has_masc_vibes(i.author)]
    other = [i for _, i in p if not has_fem_vibes(i.author) and not has_masc_vibes(i.author)]
    lists = [transfem, cisfem, transmasc, cismasc, other]
    for i in range(max(map(len, lists))):
        for x in lists:
            if i < len(x):
                yield x[i]


def gender_splitmix(p: List[Tuple[float, PostWithInfo]]) -> List[PostWithInfo]:
    return list(_gender_splitmix(p))


def top_100_chronological(p: List[Tuple[float, PostWithInfo]]) -> List[PostWithInfo]:
    return sorted(
        [i for _, i in p][:100],
        key=lambda p: p.post.indexed_at,
        reverse=True
    )


async def create_feed(db: Database, fp: FeedParameters, rd: RunDetails) -> None:

    posts_with_scores = [
        (fp.score_func(rd.run_starttime, p), p)
        for p in rd.candidate_posts
        if fp.filter(p)
    ]

    posts_by_author: Dict[str, List[Tuple[float, PostWithInfo]]] = groupby(
        lambda t: t[1].author.did, posts_with_scores
    )

    scored_posts = sorted_by_score_desc(
        [
            (post_raw_score / (2 ** index), post)
            for just_by_author in posts_by_author.values()
            for index, (post_raw_score, post) in enumerate(sorted_by_score_desc(just_by_author))
            if post_raw_score > 0
        ]
    )

    will_store = fp.remix_func(scored_posts)[:500]

    cprint(f'Scoring {fp.feed_name}::{rd.run_version} has resulted in {len(will_store)} scored posts', 'yellow', force_color=True)

    for i, post in enumerate(will_store):
        if i % 20 == 0:
            await asyncio.sleep(0.01)
        try:
            await db.postscore.create(
                data={
                    'uri': post.post.uri,
                    'version': rd.run_version,
                    # This is actually "rank"
                    'score': len(will_store) - i,
                    'created_at': rd.run_starttime,
                    'feed_name': fp.feed_name,
                }
            )
        except prisma.errors.UniqueViolationError:
            uri_count = sum(i.post.uri == post.post.uri for i in will_store)
            cprint(f'Unique PostScore violation error on {post.post.uri}::{fp.feed_name}::{rd.run_version} ({uri_count} instances of this URI)', 'red', force_color=True)


def post_is_irl_nsfw(p: PostWithInfo) -> bool:
    t = p.post.text.lower()
    return (
        len(set(p.post.labels) & {'nudity', 'suggestive', 'porn'}) > 0
        or 'nsfw' in t
        or 'murrsuit' in t
        or 'porn' in t
    ) and not (
        'art' in t
        or 'commission' in t
        or 'drawing' in t
    )


ALGORITHMIC_FEEDS = [
    FeedParameters(
        feed_name='fox-feed',
        filter=lambda p: p.author.manual_include_in_fox_feed in [None, True],
        score_func=raw_score,
    ),
    FeedParameters(
        feed_name='vix-feed',
        filter=lambda p: p.author.manual_include_in_vix_feed or (p.author.manual_include_in_vix_feed is None and p.author.autolabel_fem_vibes and not p.author.autolabel_masc_vibes),
        score_func=raw_score,
    ),
    FeedParameters(
        feed_name='fresh-feed',
        filter=lambda p: p.author.manual_include_in_vix_feed or (p.author.manual_include_in_vix_feed is None and p.author.autolabel_fem_vibes and not p.author.autolabel_masc_vibes),
        score_func=raw_freshness,
    ),
    FeedParameters(
        feed_name='vix-votes',
        filter=lambda p: p.author.manual_include_in_fox_feed in [None, True],
        score_func=raw_vix_vote_score,
    ),
    FeedParameters(
        feed_name='bisexy',
        filter=lambda p: post_is_irl_nsfw(p) and p.post.media_count > 0,
        score_func=raw_score,
        remix_func=gender_splitmix
    ),
    FeedParameters(
        feed_name='top-feed',
        filter=lambda p: p.author.manual_include_in_vix_feed or (p.author.manual_include_in_vix_feed is None and p.author.autolabel_fem_vibes and not p.author.autolabel_masc_vibes),
        score_func=lambda _, p: p.like_count_furries,
        remix_func=top_100_chronological,
    )
]


async def score_posts(db: Database) -> None:

    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f'Starting scoring round {run_version}', 'yellow', force_color=True)

    all_posts = await load_all_posts(db, run_starttime)
    total_likes = sum(i.like_count_furries for i in all_posts)
    total_girl_likes = sum(i.like_count_girls for i in all_posts)

    cprint(f'Scoring round {run_version} has {len(all_posts)} posts and {total_likes} likes and {total_girl_likes} girl-likes', 'yellow', force_color=True)

    rd = RunDetails(
        candidate_posts=all_posts,
        run_starttime=run_starttime,
        run_version=run_version
    )

    for algo in ALGORITHMIC_FEEDS:
        await create_feed(db, algo, rd)

    run_endtime = datetime.now(tz=timezone.utc)

    cprint(f'Scoring round {run_version} took {(run_endtime - run_starttime).seconds // 60} minutes', 'yellow', force_color=True)

    await db.postscore.delete_many(
        where={'created_at': {'lt': run_starttime - timedelta(hours=1)}}
    )


async def score_posts_forever(db: Database):
    while True:
        try:
            await score_posts(db)
            cprint(f'gc-d {gc.collect()} objects', 'yellow', force_color=True)
        except Exception:
            cprint(f'Error during score_posts', color='red', force_color=True)
            traceback.print_exc()
        await asyncio.sleep(60)


async def main(forever: bool):
    db = await make_database_connection(timeout=30)
    if forever:
        await score_posts_forever(db)
    else:
        await score_posts(db)


if __name__ == '__main__':
    forever = '--forever' in sys.argv
    asyncio.run(main(forever))
