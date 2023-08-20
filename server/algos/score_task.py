from collections import defaultdict
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from time import sleep

from server.database import Post, PostScore

from typing_extensions import TypedDict
from typing import Optional, List, Dict, Iterable, Tuple

from termcolor import cprint

LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=24)


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


def score_posts() -> None:

    run_starttime = datetime.now(tz=timezone.utc)
    run_version = int(run_starttime.timestamp())

    cprint(f'Starting scoring round {run_version}', 'cyan')
    
    all_posts = list(load_all_posts(run_starttime))

    cprint(f'Scoring round {run_version} has {len(all_posts)} posts', 'cyan')

    posts_by_author: Dict[str, List[Tuple[float, Post]]] = defaultdict(list)
    for post in all_posts:
        posts_by_author[post.authorId].append((raw_score(run_starttime, post), post))

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

    # We'll limit the number of posts in the feed because like...
    # maybe it's not healthy to scroll too far
    for score, post in scored_posts[:2000]:
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

    run_endtime = datetime.now(tz=timezone.utc)

    cprint(f'Scoring round {run_version} took {(run_endtime - run_starttime).seconds // 60} minutes', 'cyan')


def score_posts_forever():
    while True:
        score_posts()
        sleep(30)
