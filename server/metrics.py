import math
from server.database import Database
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Literal, Iterator, List, Optional
import prisma.types


METRICS_MAXIMUM_LOOKBACK = timedelta(days=3)


@dataclass
class FeedMetricsSlice:
    start: datetime
    end: datetime
    attributed_likes: int
    num_requests: int
    posts_served: int
    unique_viewers: int


@dataclass
class FeedMetrics:
    start: datetime
    end: datetime
    feed_name: Optional[str]
    timesliced: List[FeedMetricsSlice]


async def feed_metrics_for_timeslice(
    db: Database,
    feed_name: Optional[str],
    start: datetime,
    end: datetime,
) -> FeedMetricsSlice:
    feedname_filter: prisma.types.StringFilter = {"endswith": feed_name or ''}
    attributed_likes = await db.like.count(
        where={
            "NOT": [{"attributed_feed": None}],
            "attributed_feed": feedname_filter,
            "created_at": {"gte": start, "lt": end},
        }
    )
    num_requests = await db.servedblock.count(
        where={
            "feed_name": feedname_filter,
            "when": {"gte": start, "lt": end},
        }
    )
    posts_served = await db.servedpost.count(
        where={
            "feed_name": feedname_filter,
            "when": {"gte": start, "lt": end},
        }
    )
    unique_viewers = len(
        await db.servedblock.find_many(
            where={
                "feed_name": feedname_filter,
                "when": {"gte": start, "lt": end},
            },
            distinct=["client_did"],
        )
    )
    return FeedMetricsSlice(
        start=start,
        end=end,
        attributed_likes=attributed_likes,
        num_requests=num_requests,
        posts_served=posts_served,
        unique_viewers=unique_viewers,
    )


async def feed_metrics_for_time_range(
    db: Database,
    feed_name: Optional[str],
    start: datetime,
    end: datetime,
    interval: timedelta,
    *,
    floor_start_and_end: Literal[True] = True
) -> FeedMetrics:
    f_start = floor_datetime(start, interval)
    metrics = [
        await feed_metrics_for_timeslice(db, feed_name, i, i + interval)
        for i in daterange(f_start, end, interval)
    ]
    return FeedMetrics(
        start=f_start,
        end=f_start + interval * len(metrics),
        feed_name=feed_name,
        timesliced=metrics,
    )


def floor_datetime(dt: datetime, interval: timedelta) -> datetime:
    f: float = (dt - datetime.min) / interval
    return datetime.min + math.floor(f) * interval


def daterange(
    start: datetime, end: datetime, interval: timedelta
) -> Iterator[datetime]:
    c = start
    while c < end:
        yield c
        c = c + interval
