import asyncio
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from datetime import timezone
from datetime import timedelta

from server.database import Post, Database, make_database_connection
import prisma.errors

from typing import List, Dict, Iterable, Tuple, AsyncIterable, Callable

from termcolor import cprint
from dataclasses import dataclass


LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=12)
FRESH_CURVE_INFLECTION_POINT = timedelta(minutes=30)


def decay_curve(x: float) -> float:
    ALPHA = 1.5
    return (
        1 / (x ** ALPHA) if x > 1
        else (2 - (1 / ((2 - x) ** ALPHA)))
    )


async def load_all_posts(db: Database, run_starttime: datetime) -> AsyncIterable[Post]:
    # This is truely terrible
    chunk_size = 5000
    offset = 0
    while True:
        posts = await db.post.find_many(
            take=chunk_size,
            skip=offset,
            order=[{'indexed_at': 'desc'}, {'cid': 'desc'}],
            where={
                'AND': [
                    {'reply_root': None},
                    {'indexed_at': {'lt': run_starttime, 'gt': run_starttime - LOOKBACK_HARD_LIMIT}},
                ]
            },
            include={
                'author': True
            }
        )
        if not posts:
            break
        for i in posts:
            yield i
        offset += chunk_size


def raw_score(run_starttime: datetime, p: Post) -> float:
    # Number of likes, decaying over time
    # initial decay is much slower than the hacker news algo, but also decays to zero
    x = (run_starttime - p.indexed_at) / SCORING_CURVE_INFLECTION_POINT
    return (p.like_count ** 0.9 + 5) * decay_curve(x)


def raw_freshness(run_starttime: datetime, p: Post) -> float:
    # Number of likes, decaying over time
    # initial decay is much slower than the hacker news algo, but also decays to zero
    x = (run_starttime - p.indexed_at) / FRESH_CURVE_INFLECTION_POINT
    return (p.like_count ** 0.2 + 5) * decay_curve(x)


def take_first_n_per_feed(posts: Iterable[Tuple[float, Post]], n: int) -> Iterable[Tuple[float, Post]]:
    fox_feed = 0
    vix_feed = 0
    for i in posts:
        if i[1].author is None:
            continue
        if (i[1].author.in_fox_feed and fox_feed < n) or (i[1].author.in_vix_feed and vix_feed < n):
            yield i
        fox_feed += i[1].author.in_fox_feed
        vix_feed += i[1].author.in_vix_feed


@dataclass
class FeedParameters:
    feed_name: str
    filter: Callable[[Post], bool]
    score_func: Callable[[datetime, Post], float]


@dataclass
class RunDetails:
    candidate_posts: List[Post]
    run_starttime: datetime
    run_version: int


async def create_feed(db: Database, fp: FeedParameters, rd: RunDetails) -> None:

    posts_by_author: Dict[str, List[Tuple[float, Post]]] = defaultdict(list)
    for post in rd.candidate_posts:
        if fp.filter(post):
            rs = fp.score_func(rd.run_starttime, post)
            posts_by_author[post.authorId].append((rs, post))

    scored_posts = sorted(
        [
        (post_raw_score / (2 ** index), post)
        for just_by_author in posts_by_author.values()
        for index, (post_raw_score, post) in enumerate(sorted(just_by_author, key=lambda x: x[0], reverse=True))
        ],
        key=lambda x: x[0],
        reverse=True
    )

    will_store = scored_posts[:2000]

    cprint(f'Scoring {fp.feed_name}::{rd.run_version} has resulted in {len(will_store)} scored posts', 'yellow', force_color=True)

    for score, post in will_store:
        if post.author is None:
            continue
        try:
            await db.postscore.create(
                data={
                    'uri': post.uri,
                    'version': rd.run_version,
                    'score': score,
                    'created_at': rd.run_starttime,
                    'feed_name': fp.feed_name,
                }
            )
        except prisma.errors.UniqueViolationError:
            uri_count = sum(i.uri == post.uri for _, i in will_store)
            cprint(f'Unique PostScore violation error on {post.uri}::{fp.feed_name}::{rd.run_version} ({uri_count} instances of this URI)', 'red', force_color=True)


ALGORITHMIC_FEEDS = [
    FeedParameters(
        feed_name='fox-feed',
        filter=lambda p: p.author and p.author.in_fox_feed or False,
        score_func=raw_score,
    ),
    FeedParameters(
        feed_name='vix-feed',
        filter=lambda p: p.author and p.author.in_vix_feed or False,
        score_func=raw_score,
    ),
    FeedParameters(
        feed_name='fresh-feed',
        filter=lambda p: p.author and p.author.in_vix_feed or False,
        score_func=raw_freshness,
    ),
]


async def score_posts(db: Database, highlight_handles: List[str]) -> None:

    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f'Starting scoring round {run_version}', 'yellow', force_color=True)

    all_posts = [i async for i in load_all_posts(db, run_starttime)]

    cprint(f'Scoring round {run_version} has {len(all_posts)} posts', 'yellow', force_color=True)

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
        where={'created_at': {'lt': run_starttime - timedelta(hours=2)}}
    )


async def score_posts_forever(db: Database):
    while True:
        try:
            await score_posts(db, [])
        except Exception:
            cprint(f'Error during score_posts', color='red', force_color=True)
            traceback.print_exc()
        await asyncio.sleep(30)


async def main():
    db = await make_database_connection()
    await score_posts(db, sys.argv[1:])


if __name__ == '__main__':
    asyncio.run(main())
