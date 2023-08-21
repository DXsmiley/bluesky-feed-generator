import sys
import traceback
from collections import defaultdict
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from time import sleep

from server.database import Post, PostScore

from typing_extensions import TypedDict
from typing import Optional, List, Dict, Iterable, Tuple
import prisma.errors

from termcolor import cprint

LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=12)


def decay_curve(x: float) -> float:
    ALPHA = 1.5
    return (
        1 / (x ** ALPHA) if x > 1
        else (2 - (1 / ((2 - x) ** ALPHA)))
    )


def load_all_posts(run_starttime: datetime) -> Iterable[Post]:
    # This is truely terrible
    chunk_size = 5000
    offset = 0
    while True:
        posts = Post.prisma().find_many(
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
        yield from posts
        offset += chunk_size


def raw_score(run_starttime: datetime, p: Post) -> float:
        # Number of likes, decaying over time
        # initial decay is much slower than the hacker news algo, but also decays to zero
        x = (run_starttime - p.indexed_at) / SCORING_CURVE_INFLECTION_POINT
        return (p.like_count + 5) * decay_curve(x)


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


def score_posts(highlight_handles: List[str]) -> None:

    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f'Starting scoring round {run_version}', 'yellow', force_color=True)
    
    all_posts = list(load_all_posts(run_starttime))

    cprint(f'Scoring round {run_version} has {len(all_posts)} posts', 'yellow', force_color=True)

    posts_by_author: Dict[str, List[Tuple[float, Post]]] = defaultdict(list)
    for post in all_posts:
        rs = raw_score(run_starttime, post)
        posts_by_author[post.authorId].append((rs, post))
        if post.author is not None and post.author.handle in highlight_handles:
            print(f'- {post.author.handle} {post.media_count} - {rs:.2f} - {post.text}')

    # Decay posts by the same author to avoid clogging the feed
    scored_posts = sorted(
        [
        (post_raw_score / (2 ** index), post)
        for just_by_author in posts_by_author.values()
        for index, (post_raw_score, post) in enumerate(sorted(just_by_author, key=lambda x: x[0], reverse=True))
        ],
        key=lambda x: x[0],
        reverse=True
    )

    will_store = list(take_first_n_per_feed(scored_posts, 2000))

    cprint(f'Scoring round {run_version} has resulted in {len(will_store)} scored posts')

    for rank, (score, post) in enumerate(will_store):
        if post.author is None:
            continue
        if post.author.handle in highlight_handles:
            print(f'- {post.author.handle} {post.media_count} - {rank}::{score:.2f} - {post.text}')
        try:
            PostScore.prisma().create(
                data={
                    'uri': post.uri,
                    'version': run_version,
                    'score': score,
                    'created_at': run_starttime,
                    'in_fox_feed': post.author.in_fox_feed,
                    'in_vix_feed': post.author.in_vix_feed,
                }
            )
        except prisma.errors.UniqueViolationError:
            uri_count = sum(i.uri == post.uri for _, i in will_store)
            cprint(f'Unique PostScore violation error on {post.uri}::{run_version} ({uri_count} instances of this URI)', 'red', force_color=True)

    run_endtime = datetime.now(tz=timezone.utc)

    cprint(f'Scoring round {run_version} took {(run_endtime - run_starttime).seconds // 60} minutes', 'yellow', force_color=True)

    PostScore.prisma().delete_many(
        where={'created_at': {'lt': run_starttime - timedelta(hours=2)}}
    )


def score_posts_forever():
    while True:
        try:
            score_posts([])
        except Exception:
            cprint(f'Error during score_posts', color='red', force=True)
            traceback.print_exc()
        sleep(30)


if __name__ == '__main__':
    score_posts(sys.argv[1:])
