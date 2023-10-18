from datetime import datetime, timedelta
import time

import foxfeed.database
from foxfeed.database import Database, ScorePostsOutputModel
import foxfeed.gen.db

from typing import List, Callable, Iterator, Literal, Optional, Coroutine, Any, Tuple
from .feed_names import FeedName

import foxfeed.gender

from termcolor import cprint
from dataclasses import dataclass


LOOKBACK_HARD_LIMIT = timedelta(hours=(24 * 4))
SCORING_CURVE_INFLECTION_POINT = timedelta(hours=8)
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
    include_some_out_of_network_posts: bool
    # lmt: int
    remix_func: Callable[
        [List[PostScoreResult]], List[PostScoreResult]
    ] = do_not_remix


@dataclass
class RunDetails:
    run_starttime: datetime
    run_version: int


GeneratorType = Callable[[Database, RunDetails], Coroutine[Any, Any, List[str]]]


def sorted_by_score_desc(
    posts: List[PostScoreResult]
) -> List[PostScoreResult]:
    return sorted(posts, key=lambda x: x.score, reverse=True)


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
    alpha=ALPHA,
    beta='8 hours',
    gamma=0.9,
)


fast_time_decay = StandardDecay(
    alpha=ALPHA,
    beta='30 minutes',
    gamma=0.3,
)


def ratio_mix(*xs: Tuple[int, List[PostScoreResult]]) -> Iterator[PostScoreResult]:
    c = 0
    i = 0
    while c < len(xs):
        c = 0
        for n, l in xs:
            if n*i >= len(l):
                c += 1
            else:
                yield from l[i*n:i*n+n]
        i += 1


async def create_feed(db: Database, fp: FeedParameters, rd: RunDetails) -> List[str]:
    t0 = time.time()

    decay = fp.decay or StandardDecay(alpha=1, beta='1 hour', gamma=1)
    scored_posts_in_network = await foxfeed.gen.db.score_posts(
        db,
        alpha=decay.alpha,
        beta=decay.beta,
        gamma=decay.gamma,
        do_time_decay=fp.decay is not None,
        include_guy_posts=fp.include_guy_posts,
        include_guy_votes=fp.include_guy_votes,
        lmt=1000,
        current_time=rd.run_starttime,
        external_posts=False,
    )

    scored_posts_out_of_network = (
        [] if not fp.include_some_out_of_network_posts
        else await foxfeed.gen.db.score_posts(
            db,
            alpha=decay.alpha,
            beta=decay.beta,
            gamma=decay.gamma,
            do_time_decay=fp.decay is not None,
            include_guy_posts=False,
            include_guy_votes=fp.include_guy_votes,
            lmt=100,
            current_time=rd.run_starttime,
            external_posts=True,
        )
    )

    scored_posts = list(
        ratio_mix(
            (7, scored_posts_in_network),
            (1, scored_posts_out_of_network)
        )
    )

    ordered_posts = [i.uri for i in fp.remix_func(scored_posts)[:500]]

    pinned_posts = await db.post.find_many(
        order={'indexed_at': 'desc'},
        where={'is_pinned': True},
    )

    will_store = (
        ordered_posts[:1]
        + [i.uri for i in pinned_posts]
        + ordered_posts[1:]
    )

    elapsed = time.time() - t0

    cprint(
        f"Scored {fp.feed_name}::{rd.run_version} - {len(will_store)} posts in {elapsed} seconds",
        "yellow",
        force_color=True,
    )

    return will_store


def generator(fp: FeedParameters) -> GeneratorType:
    async def f(db: Database, rd: RunDetails) -> List[str]:
        return await create_feed(db, fp, rd)
    return f


async def _interaction_generator(db: Database, rd: RunDetails) -> List[str]:
    t0 = time.time()

    scored_posts = await foxfeed.gen.db.score_by_interactions(
        db,
        current_time=rd.run_starttime
    )

    elapsed = time.time() - t0

    cprint(
        f"Scored quotes::{rd.run_version} - {len(scored_posts)} posts in {elapsed} seconds",
        "yellow",
        force_color=True,
    )

    return [i.uri for i in scored_posts]


fox_feed = generator(
    FeedParameters(
        feed_name="fox-feed",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=True,
        remix_func=masc_nsfw_limiter(4),
        include_some_out_of_network_posts=False,
    )
)

vix_feed = generator(
    FeedParameters(
        feed_name="vix-feed",
        decay=score_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
        include_some_out_of_network_posts=False,
    )
)

fresh_feed = generator(
    FeedParameters(
        feed_name="fresh-feed",
        decay=fast_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
        include_some_out_of_network_posts=False,
    )
)

vix_votes = generator(
    FeedParameters(
        feed_name="vix-votes",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=False,
        remix_func=masc_nsfw_limiter(4),
        include_some_out_of_network_posts=True,
    )
)

top_feed = generator(
    FeedParameters(
        feed_name="top-feed",
        decay=None,
        include_guy_posts=False,
        include_guy_votes=True,
        remix_func=top_100_chronological,
        include_some_out_of_network_posts=False,
    )
)

quotes = _interaction_generator


# bisexy = generator(
#     FeedParameters(
#         feed_name="bisexy",
#         filter=lambda p: post_is_irl_nsfw(p) and p.post.media_count > 0,
#         score_func=raw_score,
#         remix_func=gender_splitmix,
#     )
# )
