from datetime import datetime, timedelta
import time

import foxfeed.database
from foxfeed.database import Database, ScorePostsOutputModel
import foxfeed.gen.db

from typing import List, Callable, Iterator, Literal, Optional, Coroutine, Any, TypeVar
from typing_extensions import LiteralString
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
    alpha=ALPHA,
    beta='8 hours',
    gamma=0.9,
)


fast_time_decay = StandardDecay(
    alpha=ALPHA,
    beta='30 minutes',
    gamma=0.3,
)


async def create_feed(db: Database, fp: FeedParameters, rd: RunDetails) -> List[str]:
    t0 = time.time()

    decay = fp.decay or StandardDecay(alpha=1, beta='1 hour', gamma=1)
    scored_posts = await foxfeed.gen.db.score_posts(
        db,
        alpha=decay.alpha,
        beta=decay.beta,
        gamma=decay.gamma,
        do_time_decay=fp.decay is not None,
        include_guy_posts=fp.include_guy_posts,
        include_guy_votes=fp.include_guy_votes,
        lmt=1000,
        current_time=rd.run_starttime
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


fox_feed = generator(
    FeedParameters(
        feed_name="fox-feed",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=True,
        remix_func=masc_nsfw_limiter(4),
    )
)

vix_feed = generator(
    FeedParameters(
        feed_name="vix-feed",
        decay=score_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
    )
)

fresh_feed = generator(
    FeedParameters(
        feed_name="fresh-feed",
        decay=fast_time_decay,
        include_guy_posts=False,
        include_guy_votes=True,
    )
)

vix_votes = generator(
    FeedParameters(
        feed_name="vix-votes",
        decay=score_time_decay,
        include_guy_posts=True,
        include_guy_votes=False,
        remix_func=masc_nsfw_limiter(4),
    )
)

top_feed = generator(
    FeedParameters(
        feed_name="top-feed",
        decay=None,
        include_guy_posts=False,
        include_guy_votes=True,
        remix_func=top_100_chronological,
    )
)


# bisexy = generator(
#     FeedParameters(
#         feed_name="bisexy",
#         filter=lambda p: post_is_irl_nsfw(p) and p.post.media_count > 0,
#         score_func=raw_score,
#         remix_func=gender_splitmix,
#     )
# )
